package services

import java.time.Instant
import java.util.UUID

import pantheon.QueryType
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import dao.Tables.{QueryHistory, QueryHistoryRow, completionStatusColumnType, instantColumnType, profile}
import management._
import play.api.libs.json._
import _root_.util.JsonSerializers.adtFormat
import QueryHistoryRepo._
import pantheon.planner.QueryPlan
import services.serializers.QuerySerializers.queryReqBaseWrites
import _root_.util.DBIOUtil.dbio

import scala.concurrent.{ExecutionContext, Future}

object QueryHistoryRepo {
  sealed trait CompletionStatus
  case object Cancelled extends CompletionStatus
  case class Failed(msg: String) extends CompletionStatus
  case object Succeeded extends CompletionStatus

  implicit val completionStatusFormat: OFormat[CompletionStatus] = adtFormat[CompletionStatus]

}
class QueryHistoryRepo(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) {

  import dbConfig.db
  import profile.api._

  def create(id: UUID, queryDescr: QueryDescription): Future[Either[String, Unit]] = {
    val timestamp = Instant.now()

    val (queryStr, tpe, schemaIdOpt, dsIdOpt) = queryDescr.query match {
      case Native(_queryStr, dataSourceId) =>
        (_queryStr, QueryHistoryRecordType.Native, None, Some(dataSourceId))
      case SchemaBased(query, schemaId) =>
        (Json.prettyPrint(queryReqBaseWrites.writes(query)), QueryHistoryRecordType.Schema, Some(schemaId), None)
    }
    db.run(
      for {
        exists <- historyRecordExists(id).result
        res <- dbio.cond(
          !exists,
          (QueryHistory +=
            QueryHistoryRow(
              id,
              timestamp,
              queryStr,
              None,
              None,
              None,
              None,
              None,
              queryDescr.catalogId,
              queryDescr.customReference,
              dsIdOpt,
              schemaIdOpt,
              tpe
            )).map(_ => Right(())),
          Left(s"query id '$id' is already registered")
        )
      } yield res
    )
  }

  def addPlans(id: UUID, plans: Plans): Future[Unit] =
    db.run(
      QueryHistory
        .filter(_.queryHistoryId === id)
        .map(q => (q.plan, q.backendLogicalPlan, q.backendPhysicalPlan))
        .update(plans.plan.map(QueryPlan.pprint(_)),
                Some(plans.backendLogicalPlan.canonical),
                Some(plans.backendPhysicalPlan.canonical))
        .map(cnt => assert(cnt == 1, s"Cannot add plans. Query history record with id $id not found"))
    )

  def find(catalogId: UUID, id: UUID): Future[Option[QueryHistoryRow]] = db.run(get(catalogId, id).result.headOption)

  def list(catalogId: UUID,
           page: Int,
           pageSize: Int,
           customRefPattern: Option[String]): Future[(Int, Seq[QueryHistoryRow])] =
    db.run(
      for {
        len <- count(catalogId).result
        lst <- QueryHistory
          .filter(
            v => v.catalogId === catalogId && customRefPattern.fold(true.bind.?)(v.customReference.like(_))
          )
          .sortBy(_.startedAt.desc) // most recent queries first
          .drop((page - 1) * pageSize)
          .take(pageSize)
          .result
      } yield len -> lst
    )

  // Warning: currently we don't prevent complete from being called more than once
  // returns false if NO history record with given id exists(change signature to Either based if more errors are possible)
  def complete(catalogId: UUID, id: UUID, status: CompletionStatus): Future[Either[String, Unit]] = {
    val timestamp = Instant.now()
    db.run(
        QueryHistory
          .filter(v => v.queryHistoryId === id && v.catalogId === catalogId)
          .map(v => v.completedAt -> v.completionStatus)
          .update(Some(timestamp) -> Some(status))
      )
      .map(updatedCnt => Either.cond(updatedCnt == 1, (), s"task $id not found in db, cannot complete it with $status"))
  }

  private val historyRecordExists = Compiled((id: Rep[UUID]) => QueryHistory.filter(_.queryHistoryId === id).exists)

  private val get = Compiled(
    (catId: Rep[UUID], id: Rep[UUID]) => QueryHistory.filter(v => v.queryHistoryId === id && v.catalogId === catId))

  private val count = Compiled((catId: Rep[UUID]) => QueryHistory.filter(_.catalogId === catId).length)
}
