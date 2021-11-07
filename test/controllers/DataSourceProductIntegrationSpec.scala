package controllers

import java.nio.file.Files
import java.util.UUID

import dao.Tables._
import play.api.test.Helpers._
import play.api.test._
import Writables.jsonWritable
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.libs.json._
import play.api.test.FakeRequest
import _root_.util.Fixtures
import _root_.util.Fixtures.pageReads
import DataSourceProductController.{DataSourceProductListResponse, DataSourceProductReq, reqRespFormat}
import controllers.helpers.CrudlActions.PagedResponse
import play.api.libs.Files.{SingletonTemporaryFileCreator, TemporaryFile}
import play.api.mvc.MultipartFormData
import play.api.mvc.MultipartFormData.{BadPart, FilePart}
import services.DataSourceProductRepo.DataSourceProductProperty
import BundledDataSourceIntegrationSpec.{getBundledProductsWithProviders, getBundledProducts}

class DataSourceProductIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {
  import profile.api._
  import Fixtures.pagedResponseReads

  def propsJson(props: Seq[DataSourceProductProperty]): String =
    props.map(p => s"""{"name": "${p.name}", "type": "${p.`type`}"}""").mkString("[", ",", "]")

  def multipartData(key: String, fileName: String, data: Array[Byte]): MultipartFormData[TemporaryFile] = {
    val tmpFile = SingletonTemporaryFileCreator.create("tmp", "x")
    tmpFile.deleteOnExit()
    Files.write(tmpFile.toPath, data)

    val filePart = FilePart[TemporaryFile](key, fileName, Option("text/plain"), tmpFile)

    MultipartFormData[TemporaryFile](Map[String, Seq[String]](), Seq(filePart), Seq[BadPart]())
  }

  "DataSourceProductController: positive cases" should {
    "get data source product" in {
      val (p, props) = createDataSourceProduct()
      val pid = p.dataSourceProductId
      val show = route(app, FakeRequest(GET, s"/dataSourceProducts/$pid")).get
      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show) mustBe
        Json.parse(
          s"""{"id":"$pid","name":"${p.name}", "description":"${p.description.get}", "isBundled":${p.isBundled},"productRoot":"${p.productRoot}",""" +
            s""""className":null, "type":"${p.`type`}", "properties": ${propsJson(props)}}""")
    }

    "create icon for data source product" in {
      val (p, props) = createDataSourceProduct()
      val pid = p.dataSourceProductId

      val b = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

      // negative test with multiple file uploads
      {
        val filePart1 = multipartData("file", "icon.png", b).files
        val body = multipartData("file", "icon.png", b).copy(files = filePart1 ++ filePart1)
        val create =
          route(app, FakeRequest(PUT, s"/dataSourceProducts/$pid/icon.png").withMultipartFormDataBody(body)).get
        status(create) mustBe UNPROCESSABLE_ENTITY
      }

      // positive test with correct key
      {
        val body = multipartData("icon", "icon.png", b)
        val create =
          route(app, FakeRequest(PUT, s"/dataSourceProducts/$pid/icon.png").withMultipartFormDataBody(body)).get
        status(create) mustBe OK
        contentType(create) mustBe Some("text/plain")
        contentAsString(create) mustBe "Icon uploaded"
      }

      val show = route(app, FakeRequest(GET, s"/dataSourceProducts/$pid/icon.png")).get
      status(show) mustBe OK
      contentType(show) mustBe Some("image/png")
      contentAsBytes(show) mustBe b
    }

    "delete data source product" in {
      val (p, _) = createDataSourceProduct()
      val pid = p.dataSourceProductId
      val delete = route(app, FakeRequest(DELETE, s"/dataSourceProducts/$pid")).get

      // check response
      status(delete) mustBe OK

      // check record has been removed
      await(db.run(DataSourceProducts.filter(_.dataSourceProductId === pid).result.headOption)) mustBe None
    }

    "list data source products" in {
      val p1 = createDataSourceProduct()
      val p2 = createDataSourceProduct()
      val list = route(app, FakeRequest(GET, "/dataSourceProducts")).get

      // check response
      status(list) mustBe OK
      contentType(list) mustBe Some("application/json")
      contentAsJson(list).as[PagedResponse[DataSourceProductReq]].data.filterNot(_.isBundled) must contain only (Seq(
        p1,
        p2).map((DataSourceProductReq.fromRow _).tupled): _*)
    }

    "list pagination" in {
      testPagination[DataSourceProductReq](app,
                                           None,
                                           "dataSourceProducts",
                                           _ => createDataSourceProduct(),
                                           () => cleanUpDynamicProducts(),
                                           Some(getBundledProducts(db).length))
    }

    "update data source products" in {
      val (p, props) = createDataSourceProduct()
      val pid = p.dataSourceProductId
      val newProp = DataSourceProductProperty("new Prop", "new Value")
      val newProps = Seq(newProp, props(2), props(1).copy(`type` = "new Type"))

      val upd = DataSourceProductReq.fromRow(p.copy(name = "new_name",
                                                    description = Some("new_descr"),
                                                    productRoot = "new product root",
                                                    className = Some("new class name")),
                                             newProps)

      val content = Json.parse(
        s"""{"id":"$pid","name":"${upd.name}","description":"${upd.description.get}", "isBundled":${upd.isBundled},"productRoot":"${upd.productRoot}",""" +
          s""""className":"${upd.className.get}", "type": "${upd.`type`}", "properties": ${propsJson(newProps)}}""")

      val update = route(app, FakeRequest(PUT, s"/dataSourceProducts/$pid").withBody(upd)).get
      status(update) mustBe OK
      contentType(update) mustBe Some("application/json")
      contentAsJson(update) mustBe content

      val show = route(app, FakeRequest(GET, s"/dataSourceProducts/$pid")).get
      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show) mustBe content
    }

    "create data source product" in {
      val p = DataSourceProductReq.fromRow(generateDataSourceProductRow("jdbc"))
      val create = route(app, FakeRequest(POST, s"/dataSourceProducts").withBody(p)).get

      status(create) mustBe CREATED
      contentType(create) mustBe Some("application/json")

      val fields = new TableFor2(
        "fieldName" -> "value",
        Json
          .obj(
            "name" -> p.name,
            "description" -> p.description,
            "isBundled" -> p.isBundled,
            "productRoot" -> p.productRoot
          )
          .value
          .toSeq: _*
      )

      val content = contentAsJson(create)
      forAll(fields)((k, v) => content \ k mustBe JsDefined(v))
    }

    "test connection successfully" in {
      val (p, _) = createDataSourceProduct()
      val pid = p.dataSourceProductId

      val props = Map(
        "url" -> "jdbc:hsqldb:res:foodmart",
        "user" -> "FOODMART",
        "password" -> "FOODMART",
        "schema" -> "foodmart"
      )

      val test = route(app, FakeRequest(POST, s"/dataSourceProducts/$pid/testConnection").withBody(props)).get
      status(test) mustBe OK
      contentType(test) mustBe Some("application/json")
      contentAsJson(test).as[ConnectionTestResult] mustBe ConnectionTestResult(true)
    }
    "test connection for bundled products" in {
      forAll(getBundledProductsWithProviders(db)) { (_, pid, props) =>
        val test = route(app, FakeRequest(POST, s"/dataSourceProducts/$pid/testConnection").withBody(props)).get
        status(test) mustBe OK
        contentType(test) mustBe Some("application/json")
        contentAsJson(test).as[ConnectionTestResult] mustBe ConnectionTestResult(true)
      }
    }
  }

  "DataSourceProductController: negative cases" should {
    "test connection failure" in {
      val (p, _) = createDataSourceProduct()
      val pid = p.dataSourceProductId

      val props = Map(
        "url" -> "jdbc:hsqldb:res:unknown",
        "user" -> "FOODMART",
        "password" -> "FOODMART"
      )

      val test = route(app, FakeRequest(POST, s"/dataSourceProducts/$pid/testConnection").withBody(props)).get
      status(test) mustBe OK
      contentType(test) mustBe Some("application/json")
      contentAsJson(test).as[ConnectionTestResult] mustBe ConnectionTestResult(
        false,
        Some("Failed to initialize pool: Database does not exists: /unknown"))
    }

  }
}
