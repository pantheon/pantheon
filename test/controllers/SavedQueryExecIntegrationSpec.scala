package controllers

import java.util.UUID
import java.util.concurrent.TimeUnit

import _root_.util.Fixtures
import akka.util.Timeout
import controllers.SavedQueryExecController.QueryParams
import dao.Tables.SavedQueriesRow
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test._

object SavedQueryExecIntegrationSpec extends Fixtures {

  def genId() = UUID.randomUUID()

  def provideBaseSchema(catId: UUID, schemaName: String = "Foodmart", dsName: String = "foodmart"): UUID = {
    val ds =
      createFoodmartDS(catId, dsName)
    val psl =
      s"""schema $schemaName (dataSource = "$dsName") {
        |  measure unitSales(column = "sales_fact_1998.unit_sales") {
        |    metadata(name = "Unit Sales", description = "My unit sales",
        |      tags = ["a", "b", "c"], active = true, weight = 3.14)
        |  }
        |
        |  measure storeSales(column = "sales_fact_1998.store_sales") {
        |    metadata(name = "Store Sales", description = "My store sales")
        |  }
        |
        |  dimension Customer(table = "customer") {
        |    attribute lastName(column = "lname")
        |    attribute date(column = "date_accnt_opened")
        |  }
        |
        |  dimension Date(table = "time_by_day") {
        |    attribute date(column = "the_date")
        |  }
        |
        |  dimension Store(table = "store") {
        |    attribute name(column = "store_name")
        |    attribute coffeeBar(column = "coffee_bar")
        |  }
        |
        |  table customer {
        |    column customer_id
        |    column lname
        |    column date_accnt_opened
        |  }
        |
        |  table time_by_day {
        |    column time_id
        |    column the_date
        |  }
        |
        |  table store {
        |    column store_id
        |    column store_name
        |    column coffee_bar
        |  }
        |
        |  table sales_fact_1998 {
        |    column customer_id (tableRef = "customer.customer_id")
        |    column store_id (tableRef = "store.store_id")
        |    column time_id (tableRef = "time_by_day.time_id")
        |    column unit_sales
        |    column store_sales
        |  }
        |}
      """.stripMargin
    createSchema(catId, parsePsl(psl), Seq(ds.dataSourceId)).schemaId

  }

  def provideSimpleSavedQuery(catalogId: UUID, schemaId: UUID)=
    provideSavedQuery(catalogId, schemaId, "true", "date '2000-01-01'")

  def provideSavedQuery(catalogId: UUID, schemaId: UUID, coffeeBarParamValue: String, dateParamValue: String): SavedQueriesRow = {
    val base = Json.obj(
      "type" -> "Aggregate",
      "measures" -> List("unitSales", "storeSales"),
      "rows" -> List("Store.name"),
      "filter" -> s"Store.coffeeBar = $coffeeBarParamValue and Date.date > $dateParamValue",
      "limit" -> 50,
      "orderBy" -> List(Json.obj("name" -> "unitSales", "order" -> "Desc")),
      "topN" -> Json.obj(
        "orderBy" -> Json.arr(
          Json.obj(
            "name" -> "unitSales",
            "order" -> "Desc"
          )
        ),
        "dimensions" -> Json.arr("Store.name"),
        "n" -> 2
      )
    )
    createSavedQuery(SavedQueriesRow(schemaId, Json.stringify(base), genId(), catalogId))
  }
}
class SavedQueryExecIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {
  import SavedQueryExecIntegrationSpec._

  implicit val queryExecWrites = Json.writes[QueryParams]

  "Saved Query" should {

    "return query results with no additional parameters" in {

      val catalogId = createCatalog().catalogId

      val sqId = provideSimpleSavedQuery(catalogId, provideBaseSchema(catalogId)).savedQueryId

      val show =
          route(app,
                FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
                  .withJsonBody(Json.obj())).get

        contentAsJson(show)(Timeout(100, TimeUnit.SECONDS)) mustBe Json.parse(
        """{"columns":[{"ref":"Store.name","primitive":"string","metadata":{}},{"ref":"unitSales","values":[],"measure":"unitSales","primitive":"number"},{"ref":"storeSales","values":[],"measure":"storeSales","primitive":"number"}],"""+
          """"rows":[],"measureHeaders":[],"measures":[{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}},{"ref":"storeSales","metadata":{"name":"Store Sales","description":"My store sales"}}]}"""
        )
        status(show) mustBe OK
        contentType(show) mustBe Some("application/json")

      }
    }

  "return query results for query with additional parameters page 1 , pageSize 10" in {

    val catalogId = createCatalog().catalogId

    val sqId = provideSavedQuery(catalogId, provideBaseSchema(catalogId), ":boolVar", ":dateVar").savedQueryId


    val show =
      route(app,
        FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
          .withJsonBody(Json.obj(
            "_page" -> 1,
            "_pageSize" -> 10,
            "_queryId" -> UUID.randomUUID(),
            "_customReference" -> "XXX",
            "boolVar" -> true,
            "dateVar" -> Json.obj("type" -> "Date", "value" -> java.sql.Date.valueOf("1990-01-01"))
          ))).get

    contentAsJson(show) mustBe Json.parse(
      """{
        |  "columns" : [ {
        |    "ref" : "Store.name",
        |    "primitive" : "string",
        |    "metadata" : { }
        |  }, {
        |    "ref" : "unitSales",
        |    "values" : [ ],
        |    "measure" : "unitSales",
        |    "primitive" : "number"
        |  }, {
        |    "ref" : "storeSales",
        |    "values" : [ ],
        |    "measure" : "storeSales",
        |    "primitive" : "number"
        |  } ],
        |  "rows" : [
        |  [ "Store 12", 37680, 79045.67 ],
        |  [ "Store 8", 37143, 79063.13 ],
        |  [ "Store 19", 36643, 77931.17 ],
        |  [ "Store 17", 35444, 75219.13 ],
        |  [ "Store 13", 35346, 74965.24 ],
        |  [ "Store 21", 34343, 72383.61 ],
        |  [ "Store 15", 26672, 56579.46 ],
        |  [ "Store 24", 24222, 51620.4 ],
        |  [ "Store 4", 23752, 50047.51 ],
        |  [ "Store 6", 22707, 47843.92 ]
        |  ],
        |  "measureHeaders" : [ ],
        |  "measures" : [ {
        |    "ref" : "unitSales",
        |    "metadata" : {
        |      "weight" : 3.14,
        |      "name" : "Unit Sales",
        |      "description" : "My unit sales",
        |      "tags" : [ "a", "b", "c" ],
        |      "active" : true
        |    }
        |  }, {
        |    "ref" : "storeSales",
        |    "metadata" : {
        |      "name" : "Store Sales",
        |      "description" : "My store sales"
        |    }
        |  } ]
        |}""".stripMargin
    )
    status(show) mustBe OK
    contentType(show) mustBe Some("application/json")

  }


  "return query results for query with additional parameters page 2 , pageSize 3" in {

      val catalogId = createCatalog().catalogId

      val sqId = provideSavedQuery(catalogId, provideBaseSchema(catalogId), ":boolVar", ":dateVar").savedQueryId


      val show =
        route(app,
          FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
            .withJsonBody(Json.obj(
              "_page" -> 2,
              "_pageSize" -> 3,
              "_queryId" -> UUID.randomUUID(),
              "_customReference" -> "XXX",
              "boolVar" -> true,
              "dateVar" -> Json.obj("type" -> "Date", "value" -> java.sql.Date.valueOf("1990-01-01"))
            ))).get

      contentAsJson(show) mustBe Json.parse("""{
                            |  "columns" : [ {
                            |    "ref" : "Store.name",
                            |    "primitive" : "string",
                            |    "metadata" : { }
                            |  }, {
                            |    "ref" : "unitSales",
                            |    "values" : [ ],
                            |    "measure" : "unitSales",
                            |    "primitive" : "number"
                            |  }, {
                            |    "ref" : "storeSales",
                            |    "values" : [ ],
                            |    "measure" : "storeSales",
                            |    "primitive" : "number"
                            |  } ],
                            |  "rows" : [
                            |  [ "Store 17", 35444, 75219.13 ],
                            |  [ "Store 13", 35346, 74965.24 ],
                            |  [ "Store 21", 34343, 72383.61 ]
                            |  ],
                            |  "measureHeaders" : [ ],
                            |  "measures" : [ {
                            |    "ref" : "unitSales",
                            |    "metadata" : {
                            |      "weight" : 3.14,
                            |      "name" : "Unit Sales",
                            |      "description" : "My unit sales",
                            |      "tags" : [ "a", "b", "c" ],
                            |      "active" : true
                            |    }
                            |  }, {
                            |    "ref" : "storeSales",
                            |    "metadata" : {
                            |      "name" : "Store Sales",
                            |      "description" : "My store sales"
                            |    }
                            |  } ]
                            |}""".stripMargin)
      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")

    }


    "return 404 for non-existent query" in {
      val catalog = createCatalog()
      val inxistentId = genId()
      val show = route(app,
                       FakeRequest(POST, s"/catalogs/${catalog.catalogId}/savedQueries/${inxistentId}/execute")
                         .withJsonBody(Json.obj())).get

      status(show) mustBe NOT_FOUND
      contentType(show) mustBe Some("application/json")
      contentAsJson(show)(Timeout(200, TimeUnit.SECONDS)) mustBe Json.parse(
        s"""{"errors":["Saved query \\"$inxistentId\\" not found in catalog: ${catalog.catalogId}"]}"""
      )
    }

    "return error for query with not provided filter variables" in {
      val catalogId = createCatalog().catalogId

      val sqId = provideSavedQuery(catalogId, provideBaseSchema(catalogId), "true", ":var").savedQueryId


        val show =
          route(app,
                FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
                  .withJsonBody(Json.obj())).get

        contentAsJson(show)(Timeout(200, TimeUnit.SECONDS)) mustBe Json.parse("""{"errors":["key not found: var"]}""")
        status(show) mustBe BAD_REQUEST
        contentType(show) mustBe Some("application/json")
    }

  "return error for query with filter variables starting with '_'" in {
    val catalogId = createCatalog().catalogId

    val sqId = provideSimpleSavedQuery(catalogId, provideBaseSchema(catalogId)).savedQueryId

    val show =
      route(app,
        FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
          .withJsonBody(Json.obj("_var" -> ""))).get

    contentAsJson(show)(Timeout(200, TimeUnit.SECONDS)) mustBe Json.parse("""{"fieldErrors":{"_var":["param cannot start with '_'"]}}""")
    status(show) mustBe UNPROCESSABLE_ENTITY
    contentType(show) mustBe Some("application/json")
  }

}
