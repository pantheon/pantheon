package services

import java.util.UUID
import java.util.concurrent.ScheduledExecutorService

import dao.Tables.{profile, _}
import slick.jdbc.JdbcProfile
import cats.data.EitherT
import util.DBIOUtil._
import pantheon.util.{BoolOps, withResource}
import cats.syntax.traverse._
import cats.syntax.either._
import cats.syntax.foldable._
import cats.instances.option._
import cats.instances.either._
import cats.instances.map._
import cats.instances.string._
import cats.instances.future._
import errors.{ConcurrentModification, ConstraintViolation, RepoError}
import slick.basic.DatabaseConfig
import pantheon.{DataSource}
import pantheon.DataSource.{DriverClassLoc}
import services.CatalogRepo.CatalogId

import scala.concurrent.{ExecutionContext, Future}

object DataSourceRepo {

  type DataSourceName = String
  type DataSourceId = UUID

  def showKey(catId: UUID, id: UUID) = s"DataSource '$id' from catalog $catId"

  case class DataSourceCacheKey(dsId: UUID, productType: String, params: Map[String, String])
}

class DataSourceRepo(dataSourceProductRepo: DataSourceProductRepo,
                     hikariHousekeeperThreadPool: ScheduledExecutorService)(implicit ec: ExecutionContext,
                                                                            dbConfig: DatabaseConfig[JdbcProfile]) {
  import DataSourceRepo._
  import dbConfig.db
  import profile.api._

  @volatile
  private var dataSourceCache = Map.empty[DataSourceCacheKey, DataSource]

  def list: Future[Seq[DataSourcesRow]] = db.run(DataSources.result)

  def list(catId: CatalogId) = db.run(getDsByCatIdWithProducts(catId).result)

  def getDataSource(catId: UUID, id: UUID): Future[Option[DataSource]] = db.run(getDataSourceQuery(catId, id))

  def shutdown(): Future[Unit] = Future(
    synchronized {
      dataSourceCache.foreach { case (_, ds) => ds.close() }
    }
  )

  private def getClassLocation(prod: DataSourceProductsRow): Future[Option[DriverClassLoc]] =
    dataSourceProductRepo.getClasspath(prod).map(cp => prod.className.map(DriverClassLoc(_, cp)))

  private def createDs(name: String, `type`: String, props: Map[String, String], classLoc: Option[DriverClassLoc]) =
    DataSource.create(name, `type`, props ++ classLoc.foldMap(_.toProps), Some(hikariHousekeeperThreadPool))

  private def getOrCreateDataSource(dsr: DataSourcesRow,
                                    prod: DataSourceProductsRow): Future[Either[String, DataSource]] = {

    val key = DataSourceCacheKey(dsr.dataSourceId, prod.`type`, dsr.properties)

    getClassLocation(prod).map(classLoc =>
      dataSourceCache.get(key).map(Right(_)).getOrElse {
        synchronized {
          dataSourceCache.get(key).map(Right(_)).getOrElse {
            val res = createDs(dsr.name, prod.`type`, dsr.properties, classLoc)
            res.foreach(ds => dataSourceCache = dataSourceCache + (key -> ds))
            res
          }
        }
    })
  }

  /**
    * try create data source just to check for errors
    * data source is disposed
    */
  private def tryCreateDataSource(ds: DataSourcesRow, prod: DataSourceProductsRow): DBIO[Option[String]] =
    DBIO.from(getClassLocation(prod)).map(createDs(ds.name, prod.`type`, ds.properties, _).left.toOption)

  def testConnection(dspId: UUID, properties: Map[String, String]): Future[Option[Either[String, Boolean]]] = {
    for {
      dsRowOpt <- dataSourceProductRepo.find(dspId)
      dsOptE <- dsRowOpt.traverse {
        case (dsp, _) => getClassLocation(dsp).map(createDs("", dsp.`type`, properties, _))
      }
    } yield
      dsOptE.map(_.flatMap(ds => Either.catchNonFatal(withResource(ds)(_.testConnection())).leftMap(_.getMessage)))
  }

  def find(catId: UUID, id: UUID): Future[Option[(DataSourcesRow, DataSourceProductsRow)]] =
    db.run(dsInCatalogWithProducts(catId, id).result.headOption)

  def findDataSourceProduct(id: UUID): DBIO[Either[String, DataSourceProductsRow]] =
    dataSourceProductRepo
      .getById(id)
      .result
      .headOption
      .map(_.toRight(s"Cannot find dataSourceProduct $id"))

  // It must not be possible to delete datasources which are used in schemas
  def delete(catId: UUID, id: UUID): Future[RepoError | Boolean] = {
    val delete = for {
      usagesErr <- schemaUsagesChk(id)
      r <- dbio.cond(
        usagesErr.isEmpty,
        dsInCatalog(catId, id).delete.map(x => Right(x == 1)),
        Left(ConstraintViolation(s"cannot delete DataSource.Reason:${usagesErr.get}"))
      )
    } yield r

    dsExistsInCatalog(catId, id).result
      .flatMap(dbio.cond(_, delete, Right(false)))
      .transactionally
      .run(db)
      .detectConstraintViolation(ConcurrentModification)
  }

  /**
    * It must not be possible to update catalogId of datasource if catalog with given id does not exist
    * It must not be possible to change name or catalog id of datasource that is are used in schemas
    * It must not be possible to change name or catalog id of datasource if datasource with the same name exists in catalog
    * Constraint #2 will not hold in concurrency scenario (there is no DB-level constraint)
    */
  def update(req: DataSourcesRow): Future[RepoError | Option[(DataSourcesRow, DataSourceProductsRow)]] = {

    val (catId, id) = req.catalogId -> req.dataSourceId
    def _update(cur: DataSourcesRow): DBIO[ConstraintViolation | (DataSourcesRow, DataSourceProductsRow)] = {

      val nameDirty = cur.name != req.name
      val propsAreDirty = cur.properties != req.properties
      val dataSourceProductIsDirty = cur.dataSourceProductId != req.dataSourceProductId

      // this description include only fields that may cause an error
      def changes = show(
        nameDirty.option(s"name '${cur.name}' to '${req.name}'") ++
          propsAreDirty.option(s"properties '${cur.properties}' to '${req.properties}'") ++
          dataSourceProductIsDirty.option(
            s"dataSourceProductId '${cur.dataSourceProductId}' to '${req.dataSourceProductId}'")
      )

      for {
        usagesErr <- dbio.cond(nameDirty, schemaUsagesChk(id), None)
        nameExistsErr <- dbio.cond(
          nameDirty,
          dsNameExistsChk(req.catalogId, req.name),
          None
        )
        dsProductOrErr <- findDataSourceProduct(req.dataSourceProductId)
        dsReadErr <- dsProductOrErr.toOption
          .filter(_ => propsAreDirty || dataSourceProductIsDirty)
          .traverseDBIO(tryCreateDataSource(req, _))
          .map(_.flatten)

        errs = dsReadErr ++ usagesErr ++ nameExistsErr ++ dsProductOrErr.left.toOption
        r <- dbio.cond(
          errs.isEmpty,
          dsInCatalog(catId, id).update(req).map(_ => Right(req)),
          Left(ConstraintViolation(s"Cannot change $changes in ${showKey(catId, id)}. Reasons: ${show(errs)}"))
        )
      } yield r.map(_ -> dsProductOrErr.right.get)
    }

    dsInCatalogForUpd(catId, id).result.headOption
      .flatMap(_.traverse(v => EitherT(_update(v))).value)
      .transactionally
      .run(db)
      .detectConstraintViolation(ConcurrentModification)
  }

  /**
    * It must not be possible to create datasource with non-existent catalogId
    * It must not be possible to create datasource having existing name within catalogId
    */
  def create(req: DataSourcesRow): Future[RepoError | (DataSourcesRow, DataSourceProductsRow)] = {
    val (catId, id) = req.catalogId -> req.dataSourceId
    def _create = (DataSources returning DataSources) += req

    val result = for {
      dsExErr <- noDsExistsChk(req.dataSourceId)
      catExErr <- catalogExistsChk(req.catalogId)
      nameExErr <- dbio.cond(catExErr.isEmpty, dsNameExistsChk(req.catalogId, req.name), None)
      dsProductOrErr <- findDataSourceProduct(req.dataSourceProductId)
      dsReadErr <- dsProductOrErr.toOption.traverseDBIO(tryCreateDataSource(req, _)).map(_.flatten)
      errs = dsExErr ++ dsReadErr ++ catExErr ++ nameExErr ++ dsProductOrErr.left.toOption
      createdDs <- dbio.cond(
        errs.isEmpty,
        _create.map(Right(_)),
        Left(ConstraintViolation(s"Cannot create DataSource. Reason:${show(errs)}"))
      )
    } yield
    // TODO: manage errors together with DBIO  (avoid .right.get)
    createdDs.map(_ -> dsProductOrErr.right.get)
    result.withPinnedSession.run(db).detectConstraintViolation(ConcurrentModification)
  }

  private val dsInCatalogByName = Compiled(
    (catId: Rep[UUID], dsName: Rep[String]) => DataSources.filter(ds => ds.catalogId === catId && ds.name === dsName)
  )

  private def joinProducts(q: Query[DataSources, DataSourcesRow, Seq]) =
    q.join(DataSourceProducts).on(_.dataSourceProductId === _.dataSourceProductId)

  private[services] def createDataSourceUnsafe(ds: DataSourcesRow, prod: DataSourceProductsRow): Future[DataSource] =
    getOrCreateDataSource(ds, prod).map(_.valueOr(err =>
      throw new AssertionError(s"Cannot create datasource from database row $ds. Reason: $err")))

  private val dsInCatalog = Compiled(
    (catId: Rep[UUID], dsId: Rep[UUID]) => DataSources.filter(ds => ds.catalogId === catId && ds.dataSourceId === dsId)
  )

  private val dsInCatalogWithProducts = Compiled(
    (catId: Rep[UUID], dsId: Rep[UUID]) => getDsByCatIdWithProducts.extract(catId).filter(_._1.dataSourceId === dsId)
  )

  private val dsExistsInCatalog = Compiled(dsInCatalog.extract.tupled.andThen(_.length === 1))

  private[services] val getDataSourceQueryByName: (CatalogId, DataSourceName) => DBIO[Option[DataSource]] = {
    val c = Compiled(
      dsInCatalogByName.extract.tupled.andThen(joinProducts)
    )
    c(_, _).result.headOption.flatMap(_.traverseDBIO {
      case (ds, prod) => DBIO.from(createDataSourceUnsafe(ds, prod))
    })
  }

  private val getDataSourceQuery: (CatalogId, DataSourceId) => DBIO[Option[DataSource]] = {
    val c = Compiled(
      dsInCatalog.extract.tupled.andThen(joinProducts)
    )
    c(_, _).result.headOption.flatMap(_.traverseDBIO {
      case (ds, prod) => DBIO.from(createDataSourceUnsafe(ds, prod))
    })
  }

  private val getDsByCatId = Compiled((catId: Rep[UUID]) => DataSources.filter(_.catalogId === catId))

  private val getDsByCatIdWithProducts = Compiled(
    (catId: Rep[UUID]) =>
      DataSources
        .filter(_.catalogId === catId)
        .join(DataSourceProducts)
        .on(_.dataSourceProductId === _.dataSourceProductId)
        .map { case (ds, product) => ds -> product })

  private val schemaUsagesChk: DataSourceId => DBIO[Option[String]] = {
    val c = Compiled(
      (id: Rep[UUID]) =>
        DataSourceLinks
          .filter(_.dataSourceId === id)
          .join(Schemas)
          .on(_.schemaId === _.schemaId)
          .map(_._2.name))

    c(_).result.map(usages => usages.nonEmpty.option(s"DataSource is used in existing schemas: ${show(usages)}"))
  }
  private val catalogExistsChk: CatalogId => DBIO[Option[String]] = {
    val c = Compiled((catId: Rep[UUID]) => Catalogs.filter(_.catalogId === catId).exists)
    id =>
      c(id).result.map(e => (!e).option(s"Catalog '$id' does not exist"))
  }

  private val noDsExistsChk: UUID => DBIO[Option[String]] = {
    val c = Compiled((id: Rep[UUID]) => DataSources.filter(_.dataSourceId === id).exists)
    id =>
      c(id).result.map(_.option(s"DataSource '$id' already exists, DataSource id must be globally unique"))
  }

  private def dsNameExistsChk(catId: UUID, name: String): DBIO[Option[String]] =
    dsInCatalogByName(catId, name).result.headOption
      .map(_.map(_ => s"DataSource '$name' already exists in Catalog '$catId'"))

  private val dsInCatalogForUpd = Compiled(dsInCatalog.extract.tupled.andThen(_.forUpdate))
}
