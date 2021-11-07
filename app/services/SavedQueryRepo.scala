package services

import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import java.util.UUID

import cats.data.EitherT
import util.DBIOUtil._
import pantheon.util.BoolOps

import scala.concurrent.{ExecutionContext, Future}
import dao.Tables._
import errors.{ConstraintViolation, RepoError}
import pantheon.schema.Compatibility.getFields
import pantheon.{PantheonQuery, SqlQuery}
import play.api.libs.json._
import services.serializers.QuerySerializers.QueryReq
import cats.instances.future._
import pantheon.schema.Schema

object SavedQueryRepo {

  import serializers.QuerySerializers.queryReqFormat
  def toSavedQuery(row: SavedQueriesRow): SavedQuery =
    SavedQuery(
      row.savedQueryId,
      row.catalogId,
      row.schemaId,
      Json.parse(row.query).as[QueryReq],
      row.name,
      row.description
    )

  def fromSavedQuery(sq: SavedQuery): SavedQueriesRow =
    SavedQueriesRow(sq.schemaId,
                    Json.prettyPrint(queryReqFormat.writes(sq.query)),
                    sq.id,
                    sq.catalogId,
                    sq.name,
                    sq.description)

  type SavedQueryId = UUID
}

class SavedQueryRepo(schemaRepo: SchemaRepo)(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) {
  import dbConfig.db
  import profile.api._
  import SavedQueryRepo._

  def list: Future[Seq[SavedQuery]] = db.run(SavedQueries.result).map(_.map(toSavedQuery))

  def list(catalogId: UUID): Future[Seq[SavedQuery]] = db.run(getByCatalog(catalogId).result).map(_.map(toSavedQuery))

  def find(catalogId: UUID, id: UUID): Future[Option[SavedQuery]] =
    db.run(getById(catalogId, id).result.headOption).map(_.map(toSavedQuery))

  def delete(catalogId: UUID, id: UUID): Future[RepoError | Boolean] =
    db.run(getById(catalogId, id).delete.map(x => Right(x == 1))).detectConstraintViolation()

  private def validate(savedQuery: SavedQuery): EitherT[Future, ConstraintViolation, Unit] = {
    (for {
      base <- EitherT.fromEither[Future](
        savedQuery.query.toQuery.left.map("base query is invalid: " + _)
      )

      schema <- EitherT.fromOptionF[Future, String, Schema](
        schemaRepo.findAndCompile(savedQuery.catalogId, savedQuery.schemaId),
        s"schema ${savedQuery.schemaId} not found in catalog ${savedQuery.catalogId}"
      )

      _ <- base match {
        case q: PantheonQuery if getFields(schema.getCompatibility(q)).isEmpty =>
          EitherT.leftT[Future, Unit](
            s"base query is not compatible with schema ${savedQuery.schemaId}"
          )
        case _: SqlQuery | _: PantheonQuery => EitherT.pure[Future, String](())
      }
    } yield ()).leftMap(ConstraintViolation(_))
  }

  def create(savedQuery: SavedQuery): Future[RepoError | SavedQuery] = {
    def _create =
      (SavedQueries returning SavedQueries) += fromSavedQuery(savedQuery)

    validate(savedQuery)
      .semiflatMap(_ => db.run(_create.map(toSavedQuery)))
      .value
      .detectConstraintViolation()
  }

  //DISCUSS: returning the argument which is not supposed to be changed(consider Boolean instead)
  def update(savedQuery: SavedQuery): Future[RepoError | Option[SavedQuery]] = {
    val _update: DBIO[SavedQuery] = {
      val updRow = fromSavedQuery(savedQuery)
      getById(savedQuery.catalogId, savedQuery.id).update(updRow).map(_ => savedQuery)
    }

    val dbQuery = (
      for {
        exists <- EitherT.liftF[DBIO, RepoError, Boolean](
          existsForUpdate(savedQuery.catalogId, savedQuery.id).result
        )
        _ <- EitherT[DBIO, RepoError, Unit](
          dbio.cond(exists, DBIO.from(validate(savedQuery).value), Right(()))
        )
        res <- EitherT.liftF[DBIO, RepoError, Option[SavedQuery]](
          DBIO.sequenceOption(exists.option(_update))
        )
      } yield res
    ).value.transactionally

    db.run(dbQuery).detectConstraintViolation()
  }

  private val getByCatalog = Compiled((catalogId: Rep[UUID]) => SavedQueries.filter(_.catalogId === catalogId))

  private val getById = Compiled(
    (catId: Rep[UUID], id: Rep[UUID]) => SavedQueries.filter(ds => ds.savedQueryId === id && ds.catalogId === catId))

  private val existsForUpdate = Compiled(getById.extract.tupled.andThen(_.forUpdate.exists))
}
