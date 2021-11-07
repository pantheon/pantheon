package services

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.regex.Pattern

import dao.Tables.{DataSourceProductProperties, _}
import errors.RepoError
import org.reflections.Reflections
import org.reflections.scanners.ResourcesScanner
import pantheon.JdbcDataSource
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import pantheon.util.BoolOps
import pantheon.util.toGroupedMap

object DataSourceProductRepo {
  object DataSourceProductProperty {
    def fromRow(row: DataSourceProductPropertiesRow): DataSourceProductProperty =
      DataSourceProductProperty(row.name, row.`type`)
  }

  case class DataSourceProductProperty(name: String, `type`: String)
}

class DataSourceProductRepo(resourceRoot: String)(implicit ec: ExecutionContext,
                                                  dbConfig: DatabaseConfig[JdbcProfile]) {
  import DataSourceProductRepo._
  import dbConfig.db
  import profile.api._

  val ResourcePattern: Pattern = Pattern.compile(".+")

  case class PathKey(productId: UUID, fileNames: Seq[String])
  val pathRegistry = new TrieMap[PathKey, Future[Path]]

  private def createProductDir(productId: UUID): Future[Path] = {
    val dir = Files.createTempDirectory(productId.toString)
    db.run(DataSourceProductJars.filter(_.dataSourceProductId === productId).result).map { files =>
      files.foreach { f =>
        val filePath = dir.resolve(f.name)
        Files.write(filePath, f.content)
      }
      dir
    }
  }

  private def getOrCreateProductDir(productId: UUID, fileNames: Seq[String]): Future[Path] = {
    pathRegistry.getOrElseUpdate(PathKey(productId, fileNames), createProductDir(productId))
  }

  private def getFiles(prodId: UUID): Future[Seq[String]] = {
    for {
      fileNames <- listFiles(prodId)
      path <- getOrCreateProductDir(prodId, fileNames)
    } yield fileNames.map(n => path.resolve(n).toString)
  }

  private def getResources(productRoot: String): Seq[String] = {
    val resRoot = resourceRoot.replace('/', '.') + "." + productRoot
    val ref = new Reflections(resRoot, new ResourcesScanner())
    ref.getResources(ResourcePattern).asScala.toSeq.sorted
  }

  def getClasspath(prod: DataSourceProductsRow): Future[Option[String]] = {
    val classPathParts =
      if (prod.isBundled) Future.successful(getResources(prod.productRoot).map(name => s"classpath:$name"))
      else getFiles(prod.dataSourceProductId).map(_.map(name => s"file://$name"))

    classPathParts.map(parts => parts.nonEmpty.option(parts.mkString(";")))
  }

  def list(): Future[Seq[(DataSourceProductsRow, Seq[DataSourceProductProperty])]] = {
    val action = DataSourceProducts
      .joinLeft(DataSourceProductProperties)
      .on { case (products, props) => products.dataSourceProductId === props.dataSourceProductId }

    db.run(action.result).map(toGroupedMap(_)(_.flatMap(_.map(DataSourceProductProperty.fromRow))).toSeq)
  }

  def find(id: UUID): Future[Option[(DataSourceProductsRow, Seq[DataSourceProductProperty])]] = {
    val action = for {
      prod <- getById(id).result.headOption
      propRows <- DataSourceProductProperties.filter(_.dataSourceProductId === id).result
    } yield prod.map(_ -> propRows.map(DataSourceProductProperty.fromRow))
    db.run(action)
  }

  def create(dataSourceProduct: DataSourceProductsRow,
             props: Seq[DataSourceProductProperty]): Future[RepoError | DataSourceProductsRow] = {
    val action = for {
      prod <- (DataSourceProducts returning DataSourceProducts) += dataSourceProduct
      propsToInsert = props.zipWithIndex.map {
        case (prop, i) =>
          DataSourceProductPropertiesRow(
            UUID.randomUUID(),
            prod.dataSourceProductId,
            prop.name,
            prop.`type`,
            i
          )
      }
      _ <- DataSourceProductProperties ++= propsToInsert
    } yield Right(prod)
    db.run(action)
  }

  private def buildPropRow(prop: DataSourceProductProperty,
                           dataSourceProductId: UUID,
                           idx: Int,
                           existing: Seq[DataSourceProductPropertiesRow]): DataSourceProductPropertiesRow = {
    existing
      .find(_.name == prop.name)
      .map(_.copy(`type` = prop.`type`, orderIndex = idx))
      .getOrElse(DataSourceProductPropertiesRow(UUID.randomUUID(), dataSourceProductId, prop.name, prop.`type`, idx))
  }

  private def updateProps(dataSourceProduct: DataSourceProductsRow,
                          props: Seq[DataSourceProductProperty]): DBIO[Unit] = {
    for {
      _ <- DataSourceProductProperties
        .filter(
          prop =>
            prop.dataSourceProductId === dataSourceProduct.dataSourceProductId &&
              !prop.name.inSet(props.map(_.name)))
        .delete
      existing <- DataSourceProductProperties
        .filter(_.dataSourceProductId === dataSourceProduct.dataSourceProductId)
        .result
      toUpsert = props.zipWithIndex.map {
        case (p, i) => buildPropRow(p, dataSourceProduct.dataSourceProductId, i, existing)
      }
      _ <- DBIO.sequence(toUpsert.map(DataSourceProductProperties.insertOrUpdate))
    } yield ()
  }

  def update(dataSourceProduct: DataSourceProductsRow,
             props: Seq[DataSourceProductProperty]): Future[Option[DataSourceProductsRow]] = {
    val action = for {
      updCount <- getById(dataSourceProduct.dataSourceProductId).update(dataSourceProduct)
      res = if (updCount == 1) Some(dataSourceProduct) else None
      _ <- res.fold[DBIO[Unit]](DBIO.successful(()))(_ => updateProps(dataSourceProduct, props))
    } yield res
    db.run(action)
  }

  def delete(id: UUID): Future[Boolean] = {
    val action = for {
      _ <- DataSourceProductProperties.filter(_.dataSourceProductId === id).delete
      count <- getById(id).delete
    } yield count
    db.run(action.map(_ == 1))
  }

  def putFile(prodId: UUID, name: String, content: Array[Byte]): Future[Boolean] = {
    db.run(DataSourceProductJars.insertOrUpdate(DataSourceProductJarsRow(prodId, name, content))).map(_ == 1)
  }

  def listFiles(prodId: UUID): Future[Seq[String]] = {
    db.run(DataSourceProductJars.filter(_.dataSourceProductId === prodId).map(_.name).sorted.result)
  }

  def deleteFile(prodId: UUID, name: String): Future[Boolean] = {
    db.run(DataSourceProductJars.filter(jar => jar.dataSourceProductId === prodId && jar.name === name).delete)
      .map(_ == 1)
  }

  def getIcon(prodId: UUID): Future[Option[Array[Byte]]] = {
    db.run(DataSourceProductIcons.filter(_.dataSourceProductId === prodId).map(_.contents).result.headOption)
  }

  def putIcon(prodId: UUID, content: Array[Byte]): Future[Boolean] = {
    db.run(DataSourceProductIcons.insertOrUpdate(DataSourceProductIconsRow(prodId, content))).map(_ == 1)
  }

  val getById = Compiled((id: Rep[UUID]) => DataSourceProducts.filter(_.dataSourceProductId === id))
}
