import java.util.UUID
import java.util.concurrent.TimeUnit

import play.api.libs.json._
import play.api.test._
import play.api.test.Helpers._
import _root_.util.Fixtures
import akka.util.Timeout
import com.zaxxer.hikari.HikariConfig
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually, scaled}
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.QueryType._
import services.CatalogRepo.CatalogId
import services.SchemaRepo.{ParsedPsl, SchemaId}
import controllers.Writables.jsonWritable
import org.scalatest.time.{Millis, Seconds, Span}
import controllers.QueryHistoryIntegrationSpec._
import controllers.SavedQueryExecIntegrationSpec

class QueryIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  import scala.language.implicitConversions

  implicit val pc = new PatienceConfig(scaled(Span(40, Seconds)), scaled(Span(700, Millis)))

  "Query" should {

    val emptyQuery: JsObject =
      Json.obj("type" -> "Aggregate", "rows" -> Seq("x"))

    "return 404 for non-existent schema" in {
      val catalog = createCatalog()
      val inxistentId = UUID.randomUUID()
      val show = route(
        app,
        FakeRequest(POST, s"/catalogs/${catalog.catalogId}/schemas/${inxistentId}/query").withJsonBody(emptyQuery)).get

      status(show) mustBe NOT_FOUND
      contentType(show) mustBe Some("application/json")
      val q = "\""
      contentAsJson(show) mustBe Json.obj("errors" -> Seq(s"schema $q$inxistentId$q not found"))
    }

    "return error for empty query" in {
      val catalog = createCatalog()
      val schema = createSchema(catalog.catalogId, emptyPsl("s1"), Seq())

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/${catalog.catalogId}/schemas/${schema.name}/query")
                .withJsonBody(emptyQuery)).get

      val res = contentAsJson(show)
      status(show) mustBe BAD_REQUEST
      contentType(show) mustBe Some("application/json")
    }

    "return query results for valid Aggregate query" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        """
          |{
          | "type": "Aggregate",
          |  "rows": ["Store.name", "Store.coffeeBar"],
          |  "measures" : ["unitSales"],
          |  "filter": "Store.coffeeBar = true",
          |  "aggregateFilter": "unitSales <= 30000",
          |  "offset": 1,
          |  "limit": 2,
          |  "orderBy": [{"name": "unitSales", "order": "Desc"}]
          |}
        """.stripMargin

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                .withJsonBody(Json.parse(query))).get

      status(show) mustBe OK

      contentType(show) mustBe Some("application/json")

      contentAsJson(show) mustBe Json.parse(
        """{"columns":[{"ref":"Store.name","primitive":"string","metadata":{}},{"ref":"Store.coffeeBar","primitive":"boolean","metadata":{}},{"ref":"unitSales","values":[],"measure":"unitSales","primitive":"number"}],"rows":[["Store 24",true,24222],["Store 4",true,23752]],"measureHeaders":[],"measures":[{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}]}"""
      )

    }

    "return query results for empty filter, not merging default filter of imported schema" in {
      val child =
        """
          schema Child(dataSource = "foodmart", strict = false){
          |  dimension Store(table = "store") {
          |    attribute number(column = "store_number")
          |  }
          |  filter  "Store.number > 25"
          }

        """.stripMargin

      val ((cid, parentId), Seq(childId)) = provideSchemaWithImports(
        ParsedPsl(child).right.get
      )

      val query =
        """
          |{
          | "type": "Aggregate",
          |  "measures" : ["unitSales"]
          |}
        """.stripMargin

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$cid/schemas/$parentId/query")
                .withJsonBody(Json.parse(query))).get

      status(show) mustBe OK

      contentType(show) mustBe Some("application/json")

      contentAsJson(show) mustBe Json.parse(
        """ {"columns":[{"ref":"unitSales","values":[],"measure":"unitSales","primitive":"number"}],"rows":[[509987]],"measureHeaders":[],"measures":[{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}]}"""
      )
    }

    "no connection leak found" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        """
          |{
          | "type": "Aggregate",
          |  "rows": [],
          |  "measures" : ["unitSales"]
          |}
        """.stripMargin

     testLeak {
        val show =
          route(app,
                FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                  .withJsonBody(Json.parse(query))).get

        status(show) mustBe OK

        contentType(show) mustBe Some("application/json")

        contentAsJson(show) mustBe Json.parse(
          """{"columns":[{"ref":"unitSales","values":[],"measure":"unitSales","primitive":"number"}],"rows":[[509987]],"measureHeaders":[],"measures":[{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}]}"""
        )
      }
    }

    "return query results for valid pivoted query" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        """
          |{
          | "type": "Aggregate",
          |  "rows": ["Store.state", "Store.city", "Store.name"],
          |  "columns": ["Store.coffeeBar", "Store.number"],
          |  "measures" : ["unitSales"],
          |  "filter" : "Store.number < 10",
          |  "offset": 1,
          |  "limit": 30,
          |  "orderBy": [{"name": "Store.name", "order": "Desc"}]
          |}
        """.stripMargin

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                .withJsonBody(Json.parse(query))).get

      status(show) mustBe OK

      contentType(show) mustBe Some("application/json")

      contentAsJson(show) mustBe Json.parse(
        """{
          |"columns":[
          |{"ref":"Store.state","primitive":"string","metadata":{}},
          |{"ref":"Store.city","primitive":"string","metadata":{}},
          |{"ref":"Store.name","primitive":"string","metadata":{}},
          |{"ref":"false->1->unitSales","values":["false","1"],"measure":"unitSales","primitive":"number"},
          |{"ref":"false->3->unitSales","values":["false","3"],"measure":"unitSales","primitive":"number"},
          |{"ref":"false->7->unitSales","values":["false","7"],"measure":"unitSales","primitive":"number"},
          |{"ref":"false->9->unitSales","values":["false","9"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->2->unitSales","values":["true","2"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->4->unitSales","values":["true","4"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->5->unitSales","values":["true","5"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->6->unitSales","values":["true","6"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->8->unitSales","values":["true","8"],"measure":"unitSales","primitive":"number"}
          |],
          |"rows":[
          |["DF","Mexico City","Store 9",null,null,null,10880,null,null,null,null,null],
          |["Yucatan","Merida","Store 8",null,null,null,null,null,null,null,null,37143],
          |["CA","Los Angeles","Store 7",null,null,24061,null,null,null,null,null,null],
          |["CA","Beverly Hills","Store 6",null,null,null,null,null,null,null,22707,null],
          |["Jalisco","Guadalajara","Store 5",null,null,null,null,null,null,2124,null,null],
          |["Zacatecas","Camacho","Store 4",null,null,null,null,null,23752,null,null,null],
          |["WA","Bremerton","Store 3",null,24069,null,null,null,null,null,null,null],
          |["WA","Bellingham","Store 2",null,null,null,null,1984,null,null,null,null],
          |["Guerrero","Acapulco","Store 1",23226,null,null,null,null,null,null,null,null]
          |],"measureHeaders":[{"ref":"Store.coffeeBar"},{"ref":"Store.number"}],
          |"measures":[
          |{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}
          |]}""".stripMargin
      )

    }

    "return query results for pivoted query where columns are from different tables which are linked via 'measure's table'" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        """
          |{
          | "type": "Aggregate",
          |  "rows": ["Store.state"],
          |  "columns": ["Store.coffeeBar", "Customer.gender"],
          |  "measures" : ["unitSales"],
          |  "limit": 5
          |}
        """.stripMargin

      val show =
        route(app,
          FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
            .withJsonBody(Json.parse(query))).get

      status(show) mustBe OK

      contentType(show) mustBe Some("application/json")

      contentAsJson(show) mustBe Json.parse(
        """{
          |"columns":[
          |{"ref":"Store.state","primitive":"string","metadata":{}},
          |{"ref":"false->F->unitSales","values":["false","F"],"measure":"unitSales","primitive":"number"},
          |{"ref":"false->M->unitSales","values":["false","M"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->F->unitSales","values":["true","F"],"measure":"unitSales","primitive":"number"},
          |{"ref":"true->M->unitSales","values":["true","M"],"measure":"unitSales","primitive":"number"}
          |],
          |"rows":[
          |["BC",null,null,22067,24090],
          |["CA",10999,13062,23430,25526],
          |["DF",4558,6322,15605,18738],
          |["Guerrero",11449,11777,null,null],
          |["Jalisco",null,null,1267,857]],
          |"measureHeaders":[
          |{"ref":"Store.coffeeBar"},
          |{"ref":"Customer.gender"}
          |],
          |"measures":[
          |{"ref":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}
          |]
          }""".stripMargin
      )
    }

    "return query results for valid record query" in {
      val (catalogId, schemaId) = provideSchema()

      val query = Json.obj(
        "type" -> "Record",
        "rows" -> List("Store.name", "Store.coffeeBar"),
        "filter" -> "Store.coffeeBar = true and Store.name = 'Store 6'",
        "offset" -> 0,
        "limit" -> 5,
        "orderBy" -> List(Json.obj("name" -> "Store.name", "order" -> "Desc"))
      )

      val show = route(app,
                       FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                         .withJsonBody(query)).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      val res = contentAsJson(show)(Timeout(200, TimeUnit.SECONDS))
      res mustBe Json.parse(
        """{"columns":[{"ref":"Store.name","primitive":"string","metadata":{}},{"ref":"Store.coffeeBar","primitive":"boolean","metadata":{}}],"rows":[["Store 6",true]],"measureHeaders":[],"measures":[]}""")

    }

    "return query results for valid SQL query" in {

      val (catalogId, schemaId) = provideSchema()
      val show =
        route(
          app,
          FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
            .withBody(Json.obj("type" -> "Sql", "sql" -> """select customer_id from customer limit 1"""))
        ).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show) mustBe Json.parse(
        """{"columns":[{"ref":"customer_id","primitive":"number"}],"rows":[[1]],"measureHeaders":[],"measures":[]}""")
    }

    "return results for schema using multiple data sources" in {
      val cid = createCatalog().catalogId
      val did1 = createFoodmartDS(cid).dataSourceId
      val did2 = createFoodmartDS(cid, "foodmart1").dataSourceId

      val schema =
        """schema Foodmart(strict = false) {
          |  dimension Store(table = "store") {
          |    level store(column = "store_id") {
          |      attribute name(column = "store_name")
          |      attribute type(column = "store_type")
          |    }
          |  }
          |
          |  dimension Customer(table = "customer") {
          |    level country
          |    level province(column = "state_province")
          |    level city
          |    level customer(column = "customer_id") {
          |      attribute firstName(column = "fname")
          |      attribute lastName(column = "lname")
          |      attribute birthDate(column = "birthdate")
          |      attribute birthYear
          |      attribute gender
          |    }
          |  }
          |
          |  measure sales(column = "sales_fact_1998.store_sales")
          |  measure cost(column = "sales_fact_1998.store_cost")
          |  measure unitSales(column = "sales_fact_1998.unit_sales")
          |
          |  table customer(dataSource = "foodmart") {
          |    column customer_id
          |    column country
          |    column state_province
          |    column city
          |    column fname
          |    column lname
          |    column birthdate
          |    column gender
          |    column birthYear(expression = "extract(year from birthdate)")
          |  }
          |
          |  table store(dataSource = "foodmart") {
          |    column store_id
          |    column store_name
          |    column store_type
          |  }
          |
          |  table sales_fact_1998(dataSource = "foodmart1") {
          |    column customer_id (tableRef = "customer.customer_id")
          |    column store_id (tableRef = "store.store_id")
          |
          |    column store_sales
          |    column store_cost
          |    column unit_sales
          |  }
          |}""".stripMargin

      val schemaId = createSchema(cid, ParsedPsl(schema).right.get, Seq(did1, did2)).schemaId

      val query =
        Json.obj(
          "type" -> "Aggregate",
          "measures" -> List("sales"),
          "rows" -> List("Store.store.name", "Customer.city"),
          "limit" -> 3,
          "orderBy" -> List(
            Json.obj("name" -> "Store.store.name", "order" -> "Asc"),
            Json.obj("name" -> "Customer.city", "order" -> "Asc")
          )
        )

      val show =
        route(app,
          FakeRequest(POST, s"/catalogs/$cid/schemas/$schemaId/query")
            .withJsonBody(query)).get

      status(show) mustBe OK

      contentType(show) mustBe Some("application/json")

      contentAsJson(show) mustBe Json.parse(
        """{"columns":[{"ref":"Store.store.name","primitive":"string","metadata":{}},{"ref":"Customer.city","primitive":"string","metadata":{}},{"ref":"sales","values":[],"measure":"sales","primitive":"number"}],"rows":[["Store 1","Acapulco",30838.58],["Store 1","La Cruz",18251.45],["Store 10","Orizaba",52142.07]],"measureHeaders":[],"measures":[{"ref":"sales","metadata":{}}]}"""
      )

    }

  }

  "Compatibility query" should {
    "nested compatibility result for simple dimension only schema" in {
      val cid = createCatalog().catalogId
      val did = createFoodmartDS(cid).dataSourceId

      val schema =
        """schema Foodmart(dataSource="foodmart", strict = false) {
          |  dimension Store(table = "store") {
          |    attribute name(column = "store_name")
          |    attribute type(column = "store_type")
          |  }
          |}""".stripMargin

      val schemaId = createSchema(cid, ParsedPsl(schema).right.get, Seq(did)).schemaId

      val query =
        Json.obj(
          "query" ->
            Json.obj(
              "type" -> "Aggregate",
              "measures" -> List.empty[String],
              "columns" -> List.empty[String],
              "rows" -> List.empty[String]
            ))

      val show =
        route(app,
          FakeRequest(POST, s"/catalogs/$cid/schemas/$schemaId/nestedCompatibility")
            .withJsonBody(query)).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show)(Timeout(200, TimeUnit.SECONDS)) mustBe Json.parse(
        """{"measures":[],"dimensions":[{"children":[{"dimension":"Store","children":[{"hierarchy":"","children":[{"level":"","children":[{"attribute":"name","metadata":{},"ref":"Store.name"},{"attribute":"type","metadata":{},"ref":"Store.type"}]}]}]}]}]}""")
    }

    "return compatibility results for empty query" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        Json.obj(
          "query" ->
            Json.obj(
              "type" -> "Aggregate",
              "measures" -> List.empty[String],
              "rows" -> List.empty[String]
            ))

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/compatibility")
                .withJsonBody(query)).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      val res = contentAsJson(show)(Timeout(200, TimeUnit.SECONDS))
      res mustBe Json.parse(
        """{"measures":[{"aggregate":"Sum","ref":"storeSales","measure":"storeSales","metadata":{"name":"Store Sales","description":"My store sales"}},{"aggregate":"Sum","ref":"unitSales","measure":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}],"dimensions":[{"ref":"Store.state","dimension":"Store","hierarchy":"","level":"","attribute":"state","metadata":{}},{"ref":"Customer.gender","dimension":"Customer","hierarchy":"","level":"","attribute":"gender","metadata":{}},{"ref":"Customer.lastName","dimension":"Customer","hierarchy":"","level":"","attribute":"lastName","metadata":{}},{"ref":"Store.number","dimension":"Store","hierarchy":"","level":"","attribute":"number","metadata":{}},{"ref":"Store.city","dimension":"Store","hierarchy":"","level":"","attribute":"city","metadata":{}},{"ref":"Store.coffeeBar","dimension":"Store","hierarchy":"","level":"","attribute":"coffeeBar","metadata":{}},{"ref":"Store.name","dimension":"Store","hierarchy":"","level":"","attribute":"name","metadata":{}}]}""")
    }

    "return flat and nested compatibility results for valid query with exported schema" in {
      val catalogId = createCatalog().catalogId
      createFoodmartDS(catalogId)

      val query =
        Json.obj(
          "query" ->
            Json.obj(
              "type" -> "Aggregate",
              "measures" -> List("unitSales"),
              //fail fast when refering to fields not present in schema
              "rows" -> List("Child.Store.name"),
              "filter" -> "Child.Store.coffeeBar = true"
            ))

      val childPsl = Json.obj("psl" -> """schema Child (dataSource = "foodmart", strict = false) {
                                   |   dimension Store(table = "store") {
                                   |    attribute name(column = "store_name")
                                   |    attribute coffeeBar(column = "coffee_bar")
                                   |  }
                                   |  measure unitSales97(column = "sales_fact_1997.unit_sales")
                                   |
                                   |  measure storeSales97(column = "sales_fact_1997.store_sales")
                                   |
                                   |  table sales_fact_1997 {
                                   |    column customer_id (tableRef = "customer.customer_id")
                                   |    column store_id (tableRef = "store.store_id")
                                   |    column unit_sales
                                   |    column store_sales
                                   |  }
                                   |}
                                 """.stripMargin)

      val parentPsl = Json.obj("psl" -> """schema Foodmart (dataSource = "foodmart", strict = false) {
                                     | export Child
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
                                     |    attribute lastName(column = "lname")/**/
                                     |  }
                                     |
                                     |  table sales_fact_1998 {
                                     |    column customer_id (tableRef = "customer.customer_id")
                                     |    //TODO : Return error early if corresponding physical table not found
                                     |    column store_id (tableRef = "Child.store.store_id")
                                     |    column unit_sales
                                     |    column store_sales
                                     |  }
                                     |}
                                   """.stripMargin)

      val childResponse =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas")
                .withJsonBody(childPsl)).get
      status(childResponse) mustBe CREATED

      val parentResponse =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas")
                .withJsonBody(parentPsl)).get
      val parentId = (contentAsJson(parentResponse) \ "id").as[UUID]
      status(parentResponse) mustBe CREATED

      val showFlat =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$parentId/compatibility")
                .withJsonBody(query)).get

      status(showFlat) mustBe OK
      contentType(showFlat) mustBe Some("application/json")
      val resFlat = contentAsJson(showFlat)(Timeout(200, TimeUnit.SECONDS))
      resFlat mustBe Json.parse(
        """{
          |  "measures" : [ {
          |    "aggregate" : "Sum",
          |    "ref" : "Child.storeSales97",
          |    "schema" : "Child",
          |    "measure" : "storeSales97",
          |    "metadata" : { }
          |  }, {
          |    "aggregate" : "Sum",
          |    "ref" : "storeSales",
          |    "measure" : "storeSales",
          |    "metadata" : {
          |      "name" : "Store Sales",
          |      "description" : "My store sales"
          |    }
          |  }, {
          |    "aggregate" : "Sum",
          |    "ref" : "unitSales",
          |    "measure" : "unitSales",
          |    "metadata" : {
          |      "weight" : 3.14,
          |      "name" : "Unit Sales",
          |      "description" : "My unit sales",
          |      "tags" : [ "a", "b", "c" ],
          |      "active" : true
          |    }
          |  }, {
          |    "aggregate" : "Sum",
          |    "ref" : "Child.unitSales97",
          |    "schema" : "Child",
          |    "measure" : "unitSales97",
          |    "metadata" : { }
          |  } ],
          |  "dimensions" : [ {
          |    "ref" : "Customer.lastName",
          |    "dimension" : "Customer",
          |    "hierarchy" : "",
          |    "level" : "",
          |    "attribute" : "lastName",
          |    "metadata" : { }
          |  }, {
          |    "ref" : "Child.Store.name",
          |    "schema" : "Child",
          |    "dimension" : "Store",
          |    "hierarchy" : "",
          |    "level" : "",
          |    "attribute" : "name",
          |    "metadata" : { }
          |  }, {
          |    "ref" : "Child.Store.coffeeBar",
          |    "schema" : "Child",
          |    "dimension" : "Store",
          |    "hierarchy" : "",
          |    "level" : "",
          |    "attribute" : "coffeeBar",
          |    "metadata" : { }
          |  } ]
          |}""".stripMargin)

      val showNested =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$parentId/nestedCompatibility")
                .withJsonBody(query)).get

      status(showNested) mustBe OK
      contentType(showNested) mustBe Some("application/json")
      val resNested = contentAsJson(showNested)(Timeout(200, TimeUnit.SECONDS))
      resNested mustBe Json.parse("""{
                                    |  "measures" : [ {
                                    |    "children" : [ {
                                    |      "ref" : "storeSales",
                                    |      "measure" : "storeSales",
                                    |      "aggregate" : "Sum",
                                    |      "metadata" : {
                                    |        "name" : "Store Sales",
                                    |        "description" : "My store sales"
                                    |      }
                                    |    }, {
                                    |      "ref" : "unitSales",
                                    |      "measure" : "unitSales",
                                    |      "aggregate" : "Sum",
                                    |      "metadata" : {
                                    |        "weight" : 3.14,
                                    |        "name" : "Unit Sales",
                                    |        "description" : "My unit sales",
                                    |        "tags" : [ "a", "b", "c" ],
                                    |        "active" : true
                                    |      }
                                    |    } ]
                                    |  }, {
                                    |    "schema" : "Child",
                                    |    "children" : [ {
                                    |      "ref" : "Child.storeSales97",
                                    |      "measure" : "storeSales97",
                                    |      "aggregate" : "Sum",
                                    |      "metadata" : { }
                                    |    }, {
                                    |      "ref" : "Child.unitSales97",
                                    |      "measure" : "unitSales97",
                                    |      "aggregate" : "Sum",
                                    |      "metadata" : { }
                                    |    } ]
                                    |  } ],
                                    |  "dimensions" : [ {
                                    |    "children" : [ {
                                    |      "dimension" : "Customer",
                                    |      "children" : [ {
                                    |        "hierarchy" : "",
                                    |        "children" : [ {
                                    |          "level" : "",
                                    |          "children" : [ {
                                    |            "attribute" : "lastName",
                                    |            "metadata" : { },
                                    |            "ref" : "Customer.lastName"
                                    |          } ]
                                    |        } ]
                                    |      } ]
                                    |    } ]
                                    |  }, {
                                    |    "schema" : "Child",
                                    |    "children" : [ {
                                    |      "dimension" : "Store",
                                    |      "children" : [ {
                                    |        "hierarchy" : "",
                                    |        "children" : [ {
                                    |          "level" : "",
                                    |          "children" : [ {
                                    |            "attribute" : "name",
                                    |            "metadata" : { },
                                    |            "ref" : "Child.Store.name"
                                    |          }, {
                                    |            "attribute" : "coffeeBar",
                                    |            "metadata" : { },
                                    |            "ref" : "Child.Store.coffeeBar"
                                    |          } ]
                                    |        } ]
                                    |      } ]
                                    |    } ]
                                    |  } ]
                                    |}""".stripMargin)

    }

    "return record compatibility results for valid query" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        Json.obj(
          "query" ->
            Json.obj(
              "type" -> "Record",
              "rows" -> List("Store.name"),
              "filter" -> "Store.coffeeBar = true"
            ))
      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/compatibility")
                .withJsonBody(Json.toJson(query))).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      val res = contentAsJson(show)(Timeout(200, TimeUnit.SECONDS))
      res mustBe Json.parse(
        """{"measures":[],"dimensions":[{"ref":"Store.state","dimension":"Store","hierarchy":"","level":"","attribute":"state","metadata":{}},{"ref":"Store.number","dimension":"Store","hierarchy":"","level":"","attribute":"number","metadata":{}},{"ref":"Store.city","dimension":"Store","hierarchy":"","level":"","attribute":"city","metadata":{}},{"ref":"Store.coffeeBar","dimension":"Store","hierarchy":"","level":"","attribute":"coffeeBar","metadata":{}},{"ref":"Store.name","dimension":"Store","hierarchy":"","level":"","attribute":"name","metadata":{}}]}""")
    }

    "filter compatibility results when filters provided" in {
      val (catalogId, schemaId) = provideSchema()

      val query =
        Json.obj(
          "query" ->
            Json.obj(
              "type" -> "Aggregate",
              "measures" -> List("unitSales"),
              "rows" -> List("Store.name"),
              "filter" -> "Store.coffeeBar = true"
            ),
          "constraints" -> List("Store.", "storeSales", "unitSales")
        )

      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/compatibility")
                .withJsonBody(query)).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      val res = contentAsJson(show)(Timeout(200, TimeUnit.SECONDS))
      res mustBe Json.parse(
        """{"measures":[{"aggregate":"Sum","ref":"storeSales","measure":"storeSales","metadata":{"name":"Store Sales","description":"My store sales"}},{"aggregate":"Sum","ref":"unitSales","measure":"unitSales","metadata":{"weight":3.14,"name":"Unit Sales","description":"My unit sales","tags":["a","b","c"],"active":true}}],"dimensions":[{"ref":"Store.state","dimension":"Store","hierarchy":"","level":"","attribute":"state","metadata":{}},{"ref":"Store.number","dimension":"Store","hierarchy":"","level":"","attribute":"number","metadata":{}},{"ref":"Store.city","dimension":"Store","hierarchy":"","level":"","attribute":"city","metadata":{}},{"ref":"Store.coffeeBar","dimension":"Store","hierarchy":"","level":"","attribute":"coffeeBar","metadata":{}},{"ref":"Store.name","dimension":"Store","hierarchy":"","level":"","attribute":"name","metadata":{}}]}""")
    }
  }

  "set custom management id and custom reference to aggregateQuery, recordQuery, sqlQuery, saved query and native query" in {

    val (catalogId, schemaId) = provideSchema()

    val query1 =
      """
        |{
        |  "type" :"Aggregate",
        |  "rows": ["Store.name"],
        |}
      """.stripMargin

    val queryId = UUID.randomUUID()
    val queryref = "query"
    val query =
      Json.parse(s"""{"type": "Aggregate","rows": ["Store.name"], "customReference": "$queryref", "queryId": "$queryId"}""")

    val recordQueryId = UUID.randomUUID()
    val recordQueryRef = "recordQuery"
    val recordQuery = Json.parse(
      s"""{"type": "Record", "rows": ["Store.name"],"customReference": "$recordQueryRef", "queryId": "$recordQueryId"}""")

    val sqlQueryId = UUID.randomUUID()
    val sqlQueryRef = "sqlQuery"
    val sqlQuery = Json.parse(
      s"""{"type": "Sql", "sql": "Select customer_id from customer limit 1", "customReference": "$sqlQueryRef", "queryId": "$sqlQueryId"}""")

    val sid = SavedQueryExecIntegrationSpec.provideBaseSchema(catalogId, "S1", "D1")
    val sqId = SavedQueryExecIntegrationSpec.provideSimpleSavedQuery(catalogId, sid).savedQueryId
    val savedQueryExecId = UUID.randomUUID()
    val savedQueryExecRef = "savedQuery"
    val savedQueryExec = Json.obj(
      "_customReference" -> savedQueryExecRef,
      "_queryId" -> savedQueryExecId,
    )

    val ds = createFoodmartDS(catalogId, "D2")
    val nativeQueryId = UUID.randomUUID()
    val nativeQueryRef = "native"
    val nativeQuery = Json.obj(
      "customReference" -> nativeQueryRef,
      "queryId" -> nativeQueryId,
      "query" -> "select * from \"foodmart\".\"department\" limit 1"
    )

    val q =
      route(app,
            FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
              .withJsonBody(query)).get

    status(q) mustBe 200

    val entQ =
      route(app,
            FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
              .withJsonBody(recordQuery)).get

    status(entQ) mustBe OK

    val sqlQ =
      route(app,
            FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
              .withJsonBody(sqlQuery)).get

    status(sqlQ) mustBe OK

    val savedQ = route(app,
                       FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
                         .withJsonBody(savedQueryExec)).get

    status(savedQ) mustBe OK

    val nativeQ = route(app,
                        FakeRequest(POST, s"/catalogs/$catalogId/dataSources/${ds.dataSourceId}/nativeQuery")
                          .withJsonBody(nativeQuery)).get

    status(nativeQ) mustBe OK

    val history = eventually {
      val history = getRegisteredQueries(app, catalogId)
      history.size mustBe 5
      history
    }

    val ids = history.map(_.id)
    ids.count(_ == queryId) mustBe 1
    ids.count(_ == recordQueryId) mustBe 1
    ids.count(_ == sqlQueryId) mustBe 1
    ids.count(_ == savedQueryExecId) mustBe 1
    ids.count(_ == nativeQueryId) mustBe 1

    val refs = history.flatMap(_.customReference)
    refs.count(_ == queryref) mustBe 1
    refs.count(_ == recordQueryRef) mustBe 1
    refs.count(_ == sqlQueryRef) mustBe 1
    refs.count(_ == savedQueryExecRef) mustBe 1
    refs.count(_ == nativeQueryRef) mustBe 1
  }

  "BAD_REQUEST on duplicate management id (must be unique across all queries: aggregate, record, sql, native, saved)" in {

    val (catalogId, schemaId) = provideSchema()

    val query1 =
      """
        |{ "type" :"Aggregate",
        |  "rows": ["Store.name"],
        |}
      """.stripMargin

    val queryId = UUID.randomUUID()

    val query = Json.parse(s"""{"type": "Aggregate","rows": ["Store.name"], "queryId": "$queryId"}""")

    val recordQuery = Json.parse(s"""{"type": "Record",  "rows": ["Store.name"], "queryId": "$queryId"}""")

    val sqlQuery = Json.parse(
      s"""{"type": "Sql","sql": "Select customer_id from foodmart.customer limit 1",  "queryId": "$queryId"}""")

    val sid = SavedQueryExecIntegrationSpec.provideBaseSchema(catalogId, "S11", "D11")
    val sqId = SavedQueryExecIntegrationSpec.provideSimpleSavedQuery(catalogId, sid).savedQueryId
    val savedQueryExec = Json.obj(
      "_queryId" -> queryId,
      "name" -> "coffeeBar",
      "value" -> true
    )

    val ds = createFoodmartDS(catalogId, "D22")
    val nativeQuery = Json.obj(
      "queryId" -> queryId,
      "query" -> "select * from \"foodmart\".\"department\" limit 1"
    )

    val q1 =
      route(app,
            FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
              .withJsonBody(query)).get

    status(q1) mustBe 200

    forAll(
      new TableFor2(
        "type" -> "query",
        Aggregate -> route(app,
                      FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                        .withJsonBody(query)).get,
        Record -> route(app,
                        FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                          .withJsonBody(recordQuery)).get,
        Sql -> route(app,
                     FakeRequest(POST, s"/catalogs/$catalogId/schemas/$schemaId/query")
                       .withJsonBody(sqlQuery)).get,
        "Saved" -> route(app,
                         FakeRequest(POST, s"/catalogs/$catalogId/savedQueries/$sqId/execute")
                           .withJsonBody(savedQueryExec)).get,
        Native -> route(app,
                        FakeRequest(POST, s"/catalogs/$catalogId/dataSources/${ds.dataSourceId}/nativeQuery")
                          .withJsonBody(nativeQuery)).get
      )) {
      case (_, q) =>
        status(q) mustBe BAD_REQUEST
        contentAsString(q) mustBe s"""{"errors":["query id '$queryId' is already registered"]}"""
    }
  }
}
