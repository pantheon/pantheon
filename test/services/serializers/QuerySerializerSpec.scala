package services.serializers

import org.scalatest.{MustMatchers, WordSpec}
import QuerySerializers.QueryReq
import pantheon._
import play.api.libs.json.Json
import pantheon.planner.ASTBuilders._
import pantheon.util.Tap
import cats.syntax.either._

class QuerySerializerSpec extends WordSpec with MustMatchers {

  "QuerySerializer" must {
    "(de)serialize Sql query symmetrically" in {
      val queryBody = "foo"
      val query: Query = SqlQuery(queryBody)
      val queryJs = Json.obj("type" -> "Sql", "sql" -> queryBody)

      Json.toJson(QueryReq.fromQuery(SqlQuery(queryBody))) mustBe queryJs
      // converting to either to get rid of 'path' field in 'JsSuccess'
      queryJs.validate[QueryReq].asEither.flatMap(_.toQuery) mustBe Right(query)
    }

    "(de)serialize record query symmetrically" in {

      val query: RecordQuery =
        RecordQuery(
          rows = List("foo"),
          filter = Some(ref("z") > lit(true)),
          offset = 0,
          orderBy = List(OrderedColumn("foo", SortOrder.Desc))
        )

      val queryJs = Json.obj(
        "type" -> "Record",
        "rows" -> Seq("foo"),
        "filter" -> "z > true",
        "offset" -> 0,
        "orderBy" -> List(Json.obj("name" -> "foo", "order" -> "Desc"))
      )

      Json.toJson(QueryReq.fromQuery(query)) mustBe queryJs
      // converting to either to get rid of 'path' field in 'JsSuccess'
      queryJs.validate[QueryReq].asEither.flatMap(_.toQuery) mustBe Right(query)
    }

    "(de)serialize Aggregate query symmetrically" in {

      def mkQuery(columns: List[String]): PantheonQuery = {
        AggregateQuery(
          rows = List("foo"),
          measures = List("bar"),
          filter = Some(ref("z") > lit(true)),
          offset = 0,
          limit = Some(1),
          orderBy = List(OrderedColumn("foo", SortOrder.Desc)),
          columns = columns
        )
      }

      val query = mkQuery(columns = Nil)
      val queryJs = Json.obj(
        "type" -> "Aggregate",
        "rows" -> Seq("foo"),
        "measures" -> Seq("bar"),
        "filter" -> "z > true",
        "offset" -> 0,
        "limit" -> 1,
        "orderBy" -> List(Json.obj("name" -> "foo", "order" -> "Desc"))
      )

      Json.toJson(QueryReq.fromQuery(query)) mustBe queryJs

      queryJs.validate[QueryReq].asEither.flatMap(_.toQuery) mustBe Right(query)

      val pivotedQuery = mkQuery(
        columns = List("blah")
      )
      val pivotedQUeryJs = queryJs ++ Json.obj(
        "columns" -> Seq("blah")
      )

      Json.toJson(QueryReq.fromQuery(pivotedQuery)) mustBe pivotedQUeryJs
      pivotedQUeryJs.validate[QueryReq].asEither.flatMap(_.toQuery) mustBe Right(pivotedQuery)
    }
  }

  // TODO: test with all allowed fields permutations, add negative tests (cover all errors).

}
