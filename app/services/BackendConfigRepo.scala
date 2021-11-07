package services

import java.util.UUID

import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import util.DBIOUtil._
import dao.Tables._
import errors.RepoError
import pantheon.backend.Backend
import pantheon.backend.calcite.CalciteBackend
import pantheon.backend.spark.SparkBackend

import scala.concurrent.{ExecutionContext, Future}

object BackendConfigRepo {
  type BackendConfigId = UUID
}
class BackendConfigRepo(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) {
  import dbConfig.db
  import profile.api._

  def rowToBackend(row: BackendConfigsRow): Backend = {
    row.backendType match {
      case "calcite" =>
        CalciteBackend(row.params)
      case "spark" =>
        SparkBackend(row.params)
      case _ =>
        throw new Exception(s"Backend type '${row.backendType}' is not supported.")
    }
  }

  def getBackendForSchema(catalogId: UUID, schemaName: String): Future[Backend] = db.run(
    for {
      fromSchema <- backendFromSchema(catalogId, schemaName).result.headOption
      fromSchemaOrCatalog <- dbio.cond(
        fromSchema.forall(_.isEmpty),
        backendFromCatalog(catalogId).result.headOption,
        fromSchema
      )
    } yield fromSchemaOrCatalog.flatten.fold[Backend](CalciteBackend())(rowToBackend)
  )

  def list: Future[Seq[BackendConfigsRow]] = db.run(BackendConfigs.result)

  def list(catalogId: UUID): Future[Seq[BackendConfigsRow]] = db.run(getByCatalog(catalogId).result)

  def find(catId: UUID, id: UUID): Future[Option[BackendConfigsRow]] = db.run(getById(catId, id).result.headOption)

  def delete(catId: UUID, id: UUID): Future[RepoError | Boolean] =
    db.run(getById(catId, id).delete.map(x => Right(x == 1))).detectConstraintViolation()

  def create(req: BackendConfigsRow): Future[RepoError | BackendConfigsRow] =
    db.run(
        ((BackendConfigs returning BackendConfigs) += req).map(Right(_))
      )
      .detectConstraintViolation()

  def update(req: BackendConfigsRow): Future[RepoError | Option[BackendConfigsRow]] = {
    val q = for {
      exists <- existsForUpd(req.catalogId, req.backendConfigId).result
      res <- dbio.cond(
        exists,
        getById(req.catalogId, req.backendConfigId).update(req).map(_ => Some(req)),
        None
      )
    } yield Right(res)

    db.run(q).detectConstraintViolation()
  }

  private val getByCatalog = Compiled((catalogId: Rep[UUID]) => BackendConfigs.filter(_.catalogId === catalogId))
  private val getById = Compiled(
    (catId: Rep[UUID], id: Rep[UUID]) => BackendConfigs.filter(v => v.backendConfigId === id && v.catalogId === catId))

  private val existsForUpd = Compiled(getById.extract.tupled.andThen(_.forUpdate.exists))

  private val (backendFromSchema, backendFromCatalog) = {
    def joinBackendConf(q: Query[Rep[Option[UUID]], Option[UUID], Seq]) =
      q.joinLeft(BackendConfigs).on(_ === _.backendConfigId.?).map(_._2)

    val fromSchema = (catalogId: Rep[UUID], schemaName: Rep[String]) =>
      joinBackendConf(Schemas.filter(s => s.catalogId === catalogId && s.name === schemaName).map(_.backendConfigId))

    val fromCatalog = (catalogId: Rep[UUID]) =>
      joinBackendConf(Catalogs.filter(_.catalogId === catalogId).map(_.backendConfigId))

    Compiled(fromSchema) -> Compiled(fromCatalog)
  }
}
