package controllers

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers._
import util.Fixtures
import java.net.URI
import java.util.UUID

import controllers.NativeQueriesController.NativeQueryRequest
import controllers.Writables.jsonWritable
import play.api.http.ContentTypes

class NativeQueriesIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  implicit val nqw = Json.writes[NativeQueryRequest]
  def provideBasicSchema() = createFoodMartBasedSchemaReturningAll(
    "Foodmart",
    """schema Foodmart (dataSource = "foodmart")"""
  )

  "negative cases" should {
    "catalog not found" in {

      val inexistentId = UUID.randomUUID()
      val req =
        route(
          app,
          FakeRequest(POST, s"/catalogs/${inexistentId}/dataSources/${inexistentId}/nativeQuery")
            .withBody(NativeQueryRequest("select * from dales_fact_1997 limit 5"))
        ).get

      contentAsJson(req) mustBe Json.obj(
        "errors" -> Seq(s"Data source $inexistentId not found in catalog: $inexistentId"))
      status(req) mustBe 404

    }

    "data source not found" in {
      val (catalog, ds, _) = provideBasicSchema()

      val inxistentId = UUID.randomUUID()

      val req =
        route(
          app,
          FakeRequest(POST, s"/catalogs/${catalog.catalogId}/dataSources/${inxistentId}/nativeQuery")
            .withBody(NativeQueryRequest("select * from dales_fact_1997 limit 5"))
        ).get

      status(req) mustBe 404
      contentAsJson(req) mustBe Json.obj(
        "errors" -> Seq(s"Data source ${inxistentId} not found in catalog: ${catalog.catalogId}"))

    }

    "table does not exist" in {
      val (catalog, ds, _) = provideBasicSchema()

      val req =
        route(
          app,
          FakeRequest(POST, s"/catalogs/${catalog.catalogId}/dataSources/${ds.dataSourceId}/nativeQuery")
            .withBody(NativeQueryRequest("select * from WRONGTABLE"))
        ).get

      status(req) mustBe 400
    }

    "data source is not supported" in {
      val catId = createCatalog().catalogId
      val (dsProd, _) = createDataSourceProduct("druid")
      val ds = createDataSource(catId, "ds1", Map("url" -> "sda/2", "coordinatorUrl" -> "aa/1"),
        Some(dsProd.dataSourceProductId))
      val req =
        route(app,
              FakeRequest(POST, s"/catalogs/$catId/dataSources/${ds.dataSourceId}/nativeQuery")
                .withBody(NativeQueryRequest("select * from sales_fact_1997"))).get

      status(req) mustBe 501
      contentAsJson(req) mustBe Json.obj(
        "errors" -> Seq(
          s"DataSource ${ds.dataSourceId} of type 'DruidSource' is not supported. Only JDBC datasources are supported"))
    }
  }

  "native query controller" should {

    "be able to serve native queries" in {
      val (catalog, ds, _) = provideBasicSchema()

      val req =
        route(
          app,
          FakeRequest(POST, s"/catalogs/${catalog.catalogId}/dataSources/${ds.dataSourceId}/nativeQuery")
            .withBody(NativeQueryRequest("""select * from "foodmart"."department" limit 5"""))
        ).get

      status(req) mustBe 200
      contentType(req) mustBe Some(ContentTypes.JSON)
      contentAsJson(req) mustBe Json.parse(
        """{"columns":[{"ref":"department_id","primitive":"number"},{"ref":"department_description","primitive":"string"}],"rows":[[1,"HQ General Management"],[2,"HQ Information Systems"],[3,"HQ Marketing"],[4,"HQ Human Resources"],[5,"HQ Finance and Accounting"]],"measureHeaders":[],"measures":[]}""")
    }

    "clean up connections after use" in {
      val (catalog, ds, _) = provideBasicSchema()

      for (i <- 1 until 20) {
        val req =
          route(
            app,
            FakeRequest(POST, s"/catalogs/${catalog.catalogId}/dataSources/${ds.dataSourceId}/nativeQuery")
              .withBody(NativeQueryRequest("""select * from "foodmart"."department" limit 5"""))
          ).get

        status(req) mustBe 200
        contentType(req) mustBe Some(ContentTypes.JSON)
        contentAsJson(req) mustBe Json.parse(
          """{"columns":[{"ref":"department_id","primitive":"number"},{"ref":"department_description","primitive":"string"}],"rows":[[1,"HQ General Management"],[2,"HQ Information Systems"],[3,"HQ Marketing"],[4,"HQ Human Resources"],[5,"HQ Finance and Accounting"]],"measureHeaders":[],"measures":[]}""")
      }
    }

  }

}
