package controllers

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.scalatest.prop.TableDrivenPropertyChecks
import pantheon.schema.FoodmartSchemaFixture
import play.api.test.FakeRequest
import play.api.test.Helpers.{GET, route, status}
import play.api.test.Helpers._
import _root_.util.Fixtures
import com.zaxxer.hikari.HikariConfig
import org.scalatestplus.play.BaseOneAppPerSuite

class SchemaVerificationIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  val fixture = new FoodmartSchemaFixture {}
  val schemaName = "Foodmart"

  "SchemaVerificationController" should {

    "respond with 422 error and proper description in negative case" in {

      val (catId, schemaId) = createFoodMartBasedSchema(
        schemaName,
        s"""schema $schemaName (dataSource = "foodmart") {
          |  measure unitSales(columns = ["sales_fact_19989.unit_sales", "sales_fact_1998.BAR"])
          |  measure linesCount(aggregate = "count", columns = ["sales_fact_19989.time_id", "sales_fact_1998.FOO"])
          |
          |  table sales_fact_19989 {
          |    column time_id
          |    column unit_sales
          |  }
          |  table sales_fact_1998 {
          |    column FOO
          |    column BAR
          |  }
          |  table foo(sql = "wrongsql")
          |}
        """.stripMargin
      )
      val verify = route(app, FakeRequest(GET, s"/catalogs/$catId/schemas/$schemaId/verify")).get

      status(verify) mustBe 422
      contentAsString(verify)(Timeout(5, TimeUnit.SECONDS)).mustBe(
        """{"tables":[""" +
          """{"name":"sales_fact_19989","definition":"physical table: 'sales_fact_19989'","dataSource":"foodmart","errors":["Table not found"]},""" +
          """{"name":"sales_fact_1998","definition":"physical table: 'sales_fact_1998'","dataSource":"foodmart","errors":["column 'FOO' is missing","column 'BAR' is missing"]},""" +
          """{"name":"foo","definition":"wrongsql","dataSource":"foodmart","errors":["Error during query execution: Non-query expression encountered in illegal context"]}]}"""
      )
    }

    def positiveCaseSchema() = createFoodMartBasedSchema(
      schemaName,
      s"""schema $schemaName (dataSource = "foodmart") {
         |  measure unitSales(column = "sales_fact_1998.unit_sales")
         |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
         |  table sales_fact_1998 {
         |     column time_id
         |    column unit_sales
         |  }
         |}
        """.stripMargin
    )

    "respond with 200 Ok with no body in positive case" in {
      val (catId, schemaId) = positiveCaseSchema()
      val verify = route(app, FakeRequest(GET, s"/catalogs/$catId/schemas/$schemaId/verify")).get
      status(verify) mustBe 200
      contentAsString(verify)(Timeout(5, TimeUnit.SECONDS)).mustBe("")
    }

    "no connection leak found" in {
      testLeak{
        val (catId, schemaId) = positiveCaseSchema()
        val verify = route(app, FakeRequest(GET, s"/catalogs/$catId/schemas/$schemaId/verify")).get
        status(verify) mustBe 200
      }
    }
  }
}
