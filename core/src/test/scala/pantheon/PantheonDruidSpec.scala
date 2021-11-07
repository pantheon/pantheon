package pantheon

import java.net.URI
import java.sql.ResultSet

import org.scalatest.{MustMatchers, WordSpec}
import pantheon.schema._
import util.{Logging, RowSetUtil}
import pantheon.planner.ASTBuilders._
import cats.syntax.either._
import pantheon.schema.parser.SchemaParser
import pantheon.util.Tap

class PantheonDruidSpec extends WordSpec with MustMatchers {

  implicit val ctx = Logging.newContext

  class Result(statement: Statement) {
    def plan: String = statement.backendLogicalPlan.toString

    def value(limit: Int = 100): String = RowSetUtil.toString(statement.execute(), limit)
  }

  val ds =
    DruidSource("druidy", new URI("http://localhost:3000"), new URI("http://localhost:3001"), false, false, false)

  "Pantheon" when {

    def test(schema: String, query: PantheonQuery)(block: Result => Unit): Unit =
      Pantheon.withConnection(new InMemoryCatalog(Map("_test_" -> schema), List(ds)), "_test_") { connection =>
        val statement = connection.createStatement(query).tap(_.mustBe('right))
        block(new Result(statement))
      }

    "querying druid" should {
      "complex query" ignore {
        val query =
          AggregateQuery(rows = List("Page.page"), filter = Some(ref("Page.page") === lit("Plastic_pollution")))

        val schema =
          """schema Wiki (dataSource = "druidy") {
            |  measure edits
            |
            |  dimension Page(table = "wikipedia") {
            |    attribute page
            |  }
            |}
            |""".stripMargin

        test(schema, query) { result =>
          println(result.plan)
          result.plan mustBe
            """LogicalAggregate(group=[{0}])
              |  LogicalProject(Page.page=[$16])
              |    LogicalFilter(condition=[=($16, 'Plastic_pollution')])
              |      DruidQuery(table=[[druidy, wikipedia]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]])
              |""".stripMargin

          result.value(5) mustBe """Page.page=Plastic_pollution"""
        }
      }
    }
  }
}
