package services

import java.util.UUID

import dao.Tables.CatalogsRow
import slick.jdbc.JdbcProfile
import dao.Tables._
import util.DBIOUtil._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import CatalogRepo._
import errors.{ConcurrentModification, ConstraintViolation, RepoError}
import slick.basic.DatabaseConfig
import pantheon.util.BoolOps

object CatalogRepo {
  type CatalogId = UUID
}

class CatalogRepo(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) {

  import dbConfig.db
  import profile.api._

  def catalogExists(catId: UUID): Future[Boolean] = db.run(catalogExistsFunc(catId).result)

  def list: Future[Seq[CatalogsRow]] = {
    db.run(Catalogs.result)
  }

  def find(id: UUID): Future[Option[CatalogsRow]] = {
    db.run(getById(id).result.headOption)
  }

  // It must not be possible to delete catalogs linked to schemas or data sources
  def delete(id: UUID): Future[RepoError | Boolean] = {

    (for {
      schemLinksErr <- schemaLinksChk(id)
      dsLinksErr <- dsLinksChk(id)
      res <- dbio.cond(
        schemLinksErr.isEmpty && dsLinksErr.isEmpty,
        getById(id).delete.map(v => Right(v == 1)),
        Left(ConstraintViolation(s"Cannot delete catalog $id. Reason: ${show(dsLinksErr ++ schemLinksErr)}"))
      )
    } yield res).withPinnedSession.run(db).detectConstraintViolation(ConcurrentModification)
  }

  def update(req: CatalogsRow): Future[Option[CatalogsRow]] =
    getById(req.catalogId).update(req).map(r => (r == 1).option(req)).run(db)

  def create(req: CatalogsRow): Future[ConstraintViolation | CatalogsRow] =
    db.run(
      for {
        catErr <- noCatalogExistsChk(req.catalogId)
        res <- catErr
          .map(err => DBIO.successful(Left(ConstraintViolation(s"Cannot create Catalog. Reason: $err"))))
          .getOrElse((Catalogs returning Catalogs).+=(req).map(Right(_)))
      } yield res
    )

  private val catalogExistsFunc = Compiled((id: Rep[UUID]) => Catalogs.filter(_.catalogId === id).exists)
  private def noCatalogExistsChk(id: UUID): DBIO[Option[String]] =
    catalogExistsFunc(id).result.map(_.option(s"Catalog '$id' already exists, Catalog id must be globally unique"))

  private val schemaLinksChk: CatalogId => DBIO[Option[String]] = {
    val c = Compiled((cid: Rep[UUID]) => Schemas.filter(_.catalogId === cid).map(_.name).forUpdate)
    c(_).result.map(schemaNames => schemaNames.nonEmpty.option(s"Used by schemas:${show(schemaNames)}"))
  }
  private val dsLinksChk: CatalogId => DBIO[Option[String]] = {
    val c = Compiled((cid: Rep[UUID]) => DataSources.filter(_.catalogId === cid).map(_.name))
    c(_).result.map(dsNames => dsNames.nonEmpty.option(s"Used by datasources:${show(dsNames)}"))
  }
  private val getById = Compiled((id: Rep[UUID]) => Catalogs.filter(_.catalogId === id))
}
