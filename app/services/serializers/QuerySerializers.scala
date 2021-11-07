package services.serializers

import java.util.UUID

import _root_.util.JsonSerializers._
import controllers.CustomRefs
import pantheon._
import pantheon.planner.Predicate
import pantheon.schema.SchemaPrinter
import pantheon.schema.parser.FilterParser
import play.api.libs.functional.syntax._
import play.api.libs.json.Json.WithDefaultValues
import play.api.libs.json.Reads.StringReads
import play.api.libs.json.Writes.StringWrites
import play.api.libs.json._

object QuerySerializers {

  trait PantheonQueryReqBase {
    def `type`: QueryType.Value

    // Common Query fields
    def rows: Option[List[String]]
    def filter: Option[Predicate]
    def offset: Option[Int]
    def limit: Option[Int]
    def orderBy: Option[List[OrderedColumn]]

    // Aggregate query fields
    def columns: Option[List[String]]
    def columnsTopN: Option[TopN]
    def rowsTopN: Option[TopN]
    def measures: Option[List[String]]
    def aggregateFilter: Option[Predicate]

    def buildPantheonQuery: PantheonQuery = {
      `type` match {
        case QueryType.Record =>
          RecordQuery(
            rows.getOrElse(Nil),
            filter,
            offset.getOrElse(0),
            limit,
            orderBy.getOrElse(Nil)
          )
        case QueryType.Aggregate =>
          AggregateQuery(
            columns.getOrElse(Nil),
            measures.getOrElse(Nil),
            rows.getOrElse(Nil),
            filter,
            offset.getOrElse(0),
            limit,
            orderBy.getOrElse(Nil),
            aggregateFilter
          )
        case _ =>
          throw new AssertionError("Unexpected query type")
      }
    }

  }

  trait QueryReqBase extends PantheonQueryReqBase {
    // SqlQuery fields
    def sql: Option[String]

    def toQuery: Either[String, Query] = {
      `type` match {
        case QueryType.Sql =>
          sql.toRight("'sql' field must be defined").map(SqlQuery(_))
        case QueryType.Record | QueryType.Aggregate =>
          val panteonQuery = buildPantheonQuery
          Query.validatePantheon(panteonQuery).toLeft(panteonQuery)
      }
    }
  }

  trait CustomRefsReqBase {
    def queryId: Option[UUID]
    def customReference: Option[String]

    def customRefs: CustomRefs = CustomRefs(queryId, customReference)
  }

  case class PantheonQueryReq(
      `type`: QueryType.Value,
      // PantheonQuery fields
      rows: Option[List[String]] = None,
      filter: Option[Predicate] = None,
      offset: Option[Int] = None,
      limit: Option[Int] = None,
      orderBy: Option[List[OrderedColumn]] = None,
      // Aggregate query fields
      columns: Option[List[String]] = None,
      columnsTopN: Option[TopN] = None,
      rowsTopN: Option[TopN] = None,
      measures: Option[List[String]] = None,
      aggregateFilter: Option[Predicate] = None
  ) extends PantheonQueryReqBase

  object QueryReq {
    def fromQuery(query: Query): QueryReq = {
      query match {
        case q: SqlQuery =>
          QueryReq(QueryType.Sql, Some(q.sql))
        case q: RecordQuery =>
          QueryReq(
            QueryType.Record,
            rows = Some(q.rows),
            filter = q.filter,
            offset = Some(q.offset),
            limit = q.limit,
            orderBy = Some(q.orderBy)
          )
        case q: AggregateQuery =>
          QueryReq(
            QueryType.Aggregate,
            rows = Some(q.rows),
            filter = q.filter,
            offset = Some(q.offset),
            limit = q.limit,
            orderBy = Some(q.orderBy),
            columns = if (q.columns.isEmpty) None else Some(q.columns),
            measures = Some(q.measures),
            aggregateFilter = q.aggregateFilter
          )
      }
    }
  }

  case class QueryReq(
      // TODO: this enum contains 'Native' value which is not suitable here.
      `type`: QueryType.Value,
      // SqlQuery fields
      sql: Option[String] = None,
      // PantheonQuery fields
      rows: Option[List[String]] = None,
      filter: Option[Predicate] = None,
      offset: Option[Int] = None,
      limit: Option[Int] = None,
      orderBy: Option[List[OrderedColumn]] = None,
      // Aggregate query fields
      columns: Option[List[String]] = None,
      columnsTopN: Option[TopN] = None,
      rowsTopN: Option[TopN] = None,
      measures: Option[List[String]] = None,
      aggregateFilter: Option[Predicate] = None
  ) extends QueryReqBase

  case class QueryReqWithRefs(
      `type`: QueryType.Value,
      // SqlQuery fields
      sql: Option[String] = None,
      // PantheonQuery fields
      rows: Option[List[String]] = None,
      filter: Option[Predicate] = None,
      offset: Option[Int] = None,
      limit: Option[Int] = None,
      orderBy: Option[List[OrderedColumn]] = None,
      // Aggregate query fields
      columns: Option[List[String]] = None,
      columnsTopN: Option[TopN] = None,
      rowsTopN: Option[TopN] = None,
      measures: Option[List[String]] = None,
      aggregateFilter: Option[Predicate] = None,
      // Custom Ref fields
      queryId: Option[UUID] = None,
      customReference: Option[String] = None
  ) extends QueryReqBase
      with CustomRefsReqBase

  // Writes are already available for all enums
  implicit val sortOrderReads = Reads.enumNameReads(SortOrder)
  implicit val queryTypeFormat = Reads.enumNameReads(QueryType)

  implicit val orderedColumnFormat: Format[OrderedColumn] = {
    ((__ \ "name").format[String] and
      (__ \ "order").formatWithDefault(SortOrder.Asc))(OrderedColumn(_, _), v => v.name -> v.order)
  }
  implicit val topNFormat: Format[TopN] = Json.format[TopN]

  implicit val predicateFormat = Format[Predicate](
    StringReads.reads(_).flatMap(FilterParser(_).fold(JsError(_), JsSuccess(_))),
    StringWrites.contramap(SchemaPrinter.printExpression)
  )

  def handleEmptyFilter[T](r: Reads[T]): Reads[T] = { jv =>
    jv.validate[JsObject]
      .flatMap { obj =>
        def removeEmpty(o: JsObject, fieldName: String): JsObject =
          o \ "filter" match {
            case JsDefined(JsString(value)) if value.trim.isEmpty =>
              o - "filter"
            case _ =>
              o
          }

        r.reads(Seq("filter", "aggregateFilter").foldLeft(obj)(removeEmpty))
      }
  }

  implicit val pantheonQueryReqFormat: Format[PantheonQueryReq] =
    Format(handleEmptyFilter(Json.reads[PantheonQueryReq]), Json.writes[PantheonQueryReq])

  implicit val queryReqFormat: Format[QueryReq] = Format(handleEmptyFilter(Json.reads[QueryReq]), Json.writes[QueryReq])
  // not implicit , will clash with queryReqFormat
  val queryReqBaseWrites: Writes[QueryReqBase] = q =>
    queryReqFormat.writes(
      QueryReq(
        `type` = q.`type`,
        sql = q.sql,
        rows = q.rows,
        filter = q.filter,
        offset = q.offset,
        limit = q.limit,
        orderBy = q.orderBy,
        columns = q.columns,
        columnsTopN = q.columnsTopN,
        rowsTopN = q.rowsTopN,
        measures = q.measures,
        aggregateFilter = q.aggregateFilter
      )
  )

  implicit val queryReqWithRefsFormat: Format[QueryReqWithRefs] =
    Format(handleEmptyFilter(Json.reads[QueryReqWithRefs]), Json.writes[QueryReqWithRefs])

}
