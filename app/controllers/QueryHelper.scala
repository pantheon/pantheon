package controllers

import java.text.SimpleDateFormat
import java.util.UUID

import cats.data.EitherT
import pantheon._
import management._
import pantheon.planner.Expression
import pantheon.schema._
import pantheon.util.Logging.{ContextLogging, LoggingContext}
import pantheon.util.withResource
import play.api.libs.json._
import play.api.mvc.Result
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import Writables.jsonWritable

import scala.concurrent.{Await, ExecutionContext, Future}
import play.api.mvc.Results.{BadRequest, ResetContent}
import services.{QueryHistoryRepo, |}
import cats.instances.option._
import cats.instances.future._
import cats.syntax.either._
import cats.syntax.traverse._
import controllers.QueryHelper.Response
import management.CancellableTasksRegistry.NotSuccessful
import pantheon.errors.InvalidQueryStructureException
import services.QueryHistoryRepo.CompletionStatus
import services.serializers.QuerySerializers.QueryReqBase
import scala.concurrent.duration._

import scala.util.Try

object QueryHelper {
  implicit val valueLiteralWrites: Writes[Value[_]] = {
    case StringValue(s)  => Json.toJson(s)
    case BooleanValue(b) => Json.toJson(b)
    case NumericValue(n) => Json.toJson(n)
    case ArrayValue(a)   => Json.toJson(a)
  }

  sealed trait ResponseMetaData
  case class DimensionColumnMetadata(ref: String, primitive: String, metadata: ValueMap) extends ResponseMetaData
  case class MeasureColumnMetadata(ref: String, values: Seq[String], measure: String, primitive: String)
      extends ResponseMetaData
  case class SqlColumnMetadata(ref: String, primitive: String) extends ResponseMetaData

  case class Ref(ref: String)
  case class MeasureMetadata(
      ref: String,
      metadata: ValueMap
  )

  case class Response(
      columns: Seq[ResponseMetaData],
      rows: Seq[JsArray],
      measureHeaders: Seq[Ref],
      measures: Seq[MeasureMetadata]
  )

  implicit val responseWrites: OWrites[Response] = {
    implicit val metaDataWrites: OWrites[ResponseMetaData] = {
      implicit val dcm: OWrites[DimensionColumnMetadata] = Json.writes[DimensionColumnMetadata]
      implicit val mcm: OWrites[MeasureColumnMetadata] = Json.writes[MeasureColumnMetadata]
      implicit val scm: OWrites[SqlColumnMetadata] = Json.writes[SqlColumnMetadata]

      {
        case v: DimensionColumnMetadata => dcm.writes(v)
        case v: MeasureColumnMetadata   => mcm.writes(v)
        case v: SqlColumnMetadata       => scm.writes(v)
      }
    }
    implicit val k: OWrites[Ref] = Json.writes[Ref]
    implicit val mm: OWrites[MeasureMetadata] = Json.writes[MeasureMetadata]
    Json.writes[Response]
  }

  def primitiveType(dataType: DataType): String = {
    dataType match {
      case ByteType | _: DecimalType | DoubleType | FloatType | IntegerType | LongType | ShortType =>
        "number"
      case BooleanType =>
        "boolean"
      case _ =>
        "string"
    }
  }

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getRow(fields: Seq[FieldDescriptor], row: Row): JsArray = {
    JsArray(fields.zipWithIndex.map {
      case (fd, i) =>
        // curried for type inference
        def toJson[T, V: Writes](v: Int => T)(f: T => V): JsValue =
          if (row.isNull(i))
            if (fd.nullable) JsNull
            else throw new AssertionError(s"got null for non-nullable field $fd")
          else Json.toJson(f(v(i)))

        fields(i).dataType match {
          case BooleanType =>
            toJson(row.getBoolean)(identity)
          case ByteType =>
            toJson(row.getByte)(identity)
          case DateType =>
            toJson(row.getDate)(dateFormat.format)
          case _: DecimalType =>
            toJson(row.getDecimal)(identity)
          case DoubleType =>
            toJson(row.getDouble)(identity)
          case FloatType =>
            toJson(row.getFloat)(identity)
          case IntegerType =>
            toJson(row.getInteger)(identity)
          case LongType =>
            toJson(row.getLong)(identity)
          case NullType =>
            JsNull
          case ObjectType =>
            toJson(row.getObject)(_.toString)
          case ShortType =>
            toJson(row.getShort)(identity)
          case StringType =>
            toJson(row.getString)(identity)
          case TimestampType =>
            toJson(row.getTimestamp)(timestampFormat.format)
        }
    })

  }
}

class QueryHelper(queryRegistry: CancellableTasksRegistry[QueryDescription], queryHistoryRepo: QueryHistoryRepo)(
    implicit ec: ExecutionContext)
    extends ContextLogging {
  // writing query history, registering query for cancellation, executing query
  def execRegistered(
      id: Option[UUID],
      queryDescr: QueryDescription,
      plans: => Option[Plans],
      // returning handled errors on the left, and the result on the right to make further transformations(e.g. to nested) possible
      cancel: => Try[Unit])(exec: => Response)(implicit ctx: LoggingContext): Future[Result | Response] = {

    val queryId = id.getOrElse(UUID.randomUUID())

    val toStatusAndResult: NotSuccessful => (CompletionStatus, Result) = {
      case CancellableTasksRegistry.Failed(err) =>
        QueryHistoryRepo.Failed(err.toString) -> BadRequest(ActionError.fromThrowable(err))
      case CancellableTasksRegistry.Cancelled => QueryHistoryRepo.Cancelled -> ResetContent
      case CancellableTasksRegistry.DuplicateTaskId(id) =>
        throw new AssertionError(
          s"Duplicate task id encountered $id. This is checked during history record creation"
        )
    }

    // writing completion to history
    def complete(status: CompletionStatus): Future[Unit] =
      queryHistoryRepo
        .complete(queryDescr.catalogId, queryId, status)
        .map(
          _.valueOr(
            // no expected way for completion to fail here
            err => throw new AssertionError(s"Cannot write completion status: $err"),
          )
        )

    (for {
      // writing initial query description to history
      _ <- EitherT(queryHistoryRepo.create(queryId, queryDescr)).leftMap(err => BadRequest(ActionError(err)))

      res <- EitherT[Future, Result, Response](
        queryRegistry
          .run(queryDescr, cancel, queryId) {
            // TODO: Implement async task registry and remove 'Await' hack
            plans.foreach(
              v =>
                Await.result(
                  // attaching plans to query in history
                  queryHistoryRepo.addPlans(queryId, v),
                  60.seconds
              ))
            // executing query
            exec
          }
          .fold(
            toStatusAndResult.andThen { case (status, result) => complete(status).map(_ => Left(result)) },
            response => complete(QueryHistoryRepo.Succeeded).map(_ => Right(response))
          )
      )
    } yield res).value
  }

  import QueryHelper._

  /**
    * Executes query against given schema
    * @param queryReq query request
    * @return play result
    */
  def executeQuery(conn: Connection,
                   catalogId: UUID,
                   schemaId: UUID,
                   queryReq: QueryReqBase,
                   queryId: Option[UUID],
                   customReference: Option[String],
                   params: Map[String, Expression] = Map.empty)(implicit ec: ExecutionContext,
                                                                dbConf: DatabaseConfig[JdbcProfile],
                                                                ctx: LoggingContext): Future[Result | Response] =
    ctx.withSpanOverFuture("QueryHelper#executeQuery") { _ =>
      handleQueryException {
        val query = queryReq.toQuery.valueOr(err => throw new InvalidQueryStructureException(err))
        // createStatement throws validation exceptions
        val stmt = conn.createStatement(query, params)

        val getPlans = stmt match {
          case s: RecordStatement =>
            () =>
              Plans(Some(s.plan), s.backendLogicalPlan, s.backendPhysicalPlan)
          case s: AggregateStatement =>
            () =>
              Plans(Some(s.plan), s.backendLogicalPlan, s.backendPhysicalPlan)
          case s: SqlStatement =>
            () =>
              Plans(None, s.backendLogicalPlan, s.backendPhysicalPlan)
          case s: PivotedStatement =>
            () =>
              Plans(None, s.backendLogicalPlan, s.backendPhysicalPlan)
        }

        execRegistered(
          queryId,
          QueryDescription(SchemaBased(queryReq, schemaId), catalogId, customReference),
          //plan, backendLogicalPlan and backendPhysicalPlan methods throw Exception
          Some(getPlans()),
          stmt.cancel()
        ) {
          withResource(stmt.execute()) { rs =>
            type ColFullName = String
            type ColKind = String

            val rows = rs.rows.map(getRow(rs.fields, _)).toList

            def getField(f: String): Field =
              conn.schema
                .getField(f)
                .getOrElse(throw new AssertionError(s"field $f not found in schema"))

            query match {
              case _: SqlQuery =>
                Response(
                  measureHeaders = Nil,
                  measures = Nil,
                  columns = rs.fields.map(f => SqlColumnMetadata(f.name, primitiveType(f.dataType))),
                  rows = rows
                )

              case q: AggregateQuery =>
                Response(
                  measureHeaders = q.columns.map(Ref(_)),
                  measures = q.measures.map(v => MeasureMetadata(v, getField(v).metadata)),
                  rows = rows,
                  columns = rs.fields.map(column =>
                    //TODO: restoring structure we had previously in the scope of this call. This is bad.
                    column.name.split("->") match {
                      case Array(dimOrMes) =>
                        getField(dimOrMes) match {
                          case m: Measure =>
                            MeasureColumnMetadata(column.name, Nil, column.name, primitiveType(column.dataType))
                          case a: Attribute =>
                            DimensionColumnMetadata(column.name, primitiveType(column.dataType), a.metadata)
                        }

                      case pivotedMeasure =>
                        MeasureColumnMetadata(column.name,
                                              pivotedMeasure.init,
                                              pivotedMeasure.last,
                                              primitiveType(column.dataType))
                  })
                )

              case q: RecordQuery =>
                Response(
                  measureHeaders = Nil,
                  measures = Nil,
                  rows = rows,
                  columns = rs.fields.map(column =>
                    getField(column.name) match {
                      case a: Attribute =>
                        DimensionColumnMetadata(column.name, primitiveType(column.dataType), a.metadata)
                      case m: Measure => throw new AssertionError(s"Measure ${m.name} found in RecordQuery")
                  })
                )
            }
          }
        }
      }.map(_.joinRight)
    }
}
