package controllers
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.scalatest.prop.TableFor3
import org.scalatestplus.play.BaseOneAppPerSuite
import BundledDataSourceIntegrationSpec.getBundledProductsWithProviders
import pantheon.schema.{DataSourceProvider, DsProviders}
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers.{POST, route}
import util.Fixtures
import slick.jdbc.PostgresProfile.api._
import dao.Tables._
import play.api.test.Helpers.{defaultAwaitTimeout => _, _}

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.util.Timeout
import services.SchemaRepo.ParsedPsl
import org.scalatest.prop.TableDrivenPropertyChecks._
import slick.jdbc.JdbcBackend

import scala.util.Random
import pantheon.util.join2Using

object BundledDataSourceIntegrationSpec {
  def getBundledProducts(db: JdbcBackend#DatabaseDef): Seq[DataSourceProductsRow] = Await.result(
    db.run(
      DataSourceProducts
        .filter(v => v.isBundled)
        .result),
    10.seconds
  )

  def getBundledProductsWithProviders(db: JdbcBackend#DatabaseDef): TableFor3[String, UUID, Map[String, String]] = {
    val bundledProductsProviders: Seq[DataSourceProvider] = DsProviders.without(DsProviders.hsqldbFoodmart)

    val bundledProducts: Seq[DataSourceProductsRow] = Await.result(
      db.run(
        DataSourceProducts
          .filter(v => v.isBundled && v.`type`.inSet(bundledProductsProviders.map(_.dsType)(collection.breakOut)))
          .result),
      10.seconds
    )
    val typeIdProps: Seq[(String, UUID, Map[String, String])] =
      join2Using(bundledProductsProviders, bundledProducts)(_.dsType, _.`type`) {
        case (provider, Seq(product)) => provider.propsOpt.map((product.`type`, product.dataSourceProductId, _))
        case x                        => throw new AssertionError(s"expecting one bundled product per provider, got $x")
      }.flatten

    Table(
      ("productType", "id", "properties"),
      typeIdProps: _*
    )
  }

}
class BundledDataSourceIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  "able to create data sources based on bundled products and run query" in {

    val bundledProducts = getBundledProductsWithProviders(db)

    implicit val timeout = Timeout(100, TimeUnit.SECONDS)

    def genName() = (0 to 15).map(_ => Random.nextPrintableChar()).filter(_.isLetterOrDigit).mkString

    val catId = createCatalog().catalogId

    def _createSchema(productId: UUID, props: Map[String, String], tableName: String): UUID = {

      val dsName = genName()
      val dsId = createDataSource(catId, dsName, props, Some(productId)).dataSourceId

      val psl = ParsedPsl(s"""schema ${genName()}(dataSource="$dsName")  {table $tableName {columns *}}""")
      psl mustBe 'right

      createSchema(
        catId,
        psl.right.get,
        Seq(dsId)
      ).schemaId
    }

    forAll(bundledProducts) { (_, productId, props) =>
      val schemaId = _createSchema(productId, props, "department")

      val res = route(
        app,
        FakeRequest(POST, s"/catalogs/$catId/schemas/$schemaId/query")
          .withJsonBody(
            Json.obj("type" -> "Sql",
                     "sql" -> """select department_id from department order by department_id limit 1""")
          )
      ).get

      val content = contentAsJson(res)
      status(res) mustBe OK
      content mustBe Json.parse("""{
                                             |  "columns" : [ {
                                             |    "ref" : "department_id",
                                             |    "primitive" : "number"
                                             |  } ],
                                             |  "rows" : [ [ 1 ] ],
                                             |  "measureHeaders" : [ ],
                                             |  "measures" : [ ]
                                             |}""".stripMargin)
    }
  }
}
