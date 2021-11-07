package controllers

import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.util.{ByteString, Timeout}
import dao.Tables.DataSourceProductsRow
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.schema.DsProviders
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers.{POST, contentType, route, status}
import util.Fixtures
import play.api.test.Helpers._
import play.api.test._

import scala.collection.JavaConverters._

class DynamicDataSourceIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  "DataSourceController: positive cases" should {
    "create and use dynamic datasource" in {

      assume(DsProviders.clickhouse.isEnabled)
      val props = DsProviders.clickhouse.propsOpt.get

      val c = createCatalog()
      val (dsProd, _) = createDataSourceProduct(
        DataSourceProductsRow(false,
                              "ch",
                              Some("ru.yandex.clickhouse.ClickHouseDriver"),
                              UUID.randomUUID(),
                              "ClickHouse Test",
                              `type` = "clickhouse"),
        generateDataSourceProductProps()
      )

      val driverDir = Paths.get(s"conf/jdbc/clickhouse")

      if (Files.isDirectory(driverDir)) {
        val files = Files.list(driverDir).iterator().asScala.toList
        if (files.isEmpty) throw new Exception(s"driver dir $driverDir is empty")
        else {
          files.foreach { f =>
            val b = Files.readAllBytes(f)
            val create =
              route(app,
                    FakeRequest(PUT, s"/dataSourceProducts/${dsProd.dataSourceProductId}/lib/${f.getFileName.toString}")
                      .withRawBody(ByteString(b))).get
            status(create) mustBe OK
          }
        }
      } else throw new Exception(s"driver dir $driverDir does not exist")

      // create datasource
      val ds = createDataSource(
        c.catalogId,
        "foodmart",
        props + ("hikaricp.maximumPoolSize" -> "3"),
        Some(dsProd.dataSourceProductId)
      )

      val psl =
        """schema Sales (dataSource = "foodmart") {
          |
          |  dimension Customer(table = "customer") {
          |    attribute country(column = "country")
          |  }
          |
          |  table customer {
          |    column country
          |  }
          |}
          |
        """.stripMargin

      // create schema
      val schema = createSchema(c.catalogId, parsePsl(psl), Seq(ds.dataSourceId))

      val q =
        """
          |{
          |  "type": "Aggregate",
          |  "rows": ["Customer.country"]
          |}
        """.stripMargin

      // run query on schema
      val show =
        route(app,
              FakeRequest(POST, s"/catalogs/${c.catalogId}/schemas/${schema.schemaId}/query")
                .withJsonBody(Json.parse(q))).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show)(Timeout(200, TimeUnit.SECONDS)) mustBe Json.parse(
        """{"columns":[{"ref":"Customer.country","primitive":"string","metadata":{}}],"rows":[["Canada"],["Mexico"],["USA"]],"measureHeaders":[],"measures":[]}"""
      )
    }
  }
}