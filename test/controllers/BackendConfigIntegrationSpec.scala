package controllers

import dao.Tables._
import play.api.test.Helpers._
import Writables.jsonWritable
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.libs.json._
import play.api.test.FakeRequest
import _root_.util.Fixtures
import Writes.UuidWrites
import BackendConfigController.{BackendConfigListResponse, BackendConfigReq, reqRespFormat}
import controllers.helpers.CrudlActions.PagedResponse

class BackendConfigIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {
  import profile.api._
  import Fixtures.pagedResponseReads

  "BackendConfigController: positive cases" should {
    "get Backend Config" in {
      val c = createCatalog()
      val bc = createBackendConfig(c.catalogId)
      val bcid = bc.backendConfigId
      val show = route(app, FakeRequest(GET, s"/catalogs/${c.catalogId}/backendConfigs/$bcid")).get
      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show) mustBe
        Json.parse(
          s"""{"id":"$bcid","catalogId":"${c.catalogId}","name":"${bc.name.get}","description":"${bc.description.get}","backendType":"${bc.backendType}",""" +
            s""""description":"${bc.description.get}","params":{"${bc.params.toSeq.head._1}":"${bc.params.toSeq.head._2}"}}""")
    }

    "delete Backend Config" in {
      val c = createCatalog()
      val bc = createBackendConfig(c.catalogId)
      val bcid = bc.backendConfigId
      val delete = route(app, FakeRequest(DELETE, s"/catalogs/${c.catalogId}/backendConfigs/$bcid")).get

      // check response
      status(delete) mustBe OK

      // check record has been removed
      await(db.run(BackendConfigs.filter(_.backendConfigId === bcid).result.headOption)) mustBe None
    }

    "list Backend Configs" in {
      val c = createCatalog()
      val bc1 = createBackendConfig(c.catalogId)
      val bc2 = createBackendConfig(c.catalogId)
      val list = route(app, FakeRequest(GET, s"/catalogs/${c.catalogId}/backendConfigs")).get

      // check response
      status(list) mustBe OK
      contentType(list) mustBe Some("application/json")
      contentAsJson(list).as[PagedResponse[BackendConfigReq]].data mustBe Seq(bc1, bc2).map(
        BackendConfigReq.fromRow)
    }

    "list pagination" in {
      val cid = createCatalog().catalogId
      testPagination[BackendConfigReq](
        app,
        Some(cid),
        "backendConfigs",
        _ => createBackendConfig(cid),
        // cannot truncate because of FK constraints
        () => await(db.run(BackendConfigs.delete))
      )
    }

    "update Backend Configs" in {
      val c = createCatalog()
      val bc = createBackendConfig(c.catalogId)
      val bcid = bc.backendConfigId

      val upd = bc.copy(name = Some("new_name"),
                        backendType = "spark",
                        description = Some("new description"),
                        params = Map("param1" -> "value1"))

      val update = route(
        app,
        FakeRequest(PUT, s"/catalogs/${c.catalogId}/backendConfigs/$bcid").withBody(BackendConfigReq.fromRow(upd))).get

      status(update) mustBe OK
      contentType(update) mustBe Some("application/json")

      contentAsJson(update) mustBe
        Json.parse(
          s"""{"id":"$bcid","catalogId":"${c.catalogId}","name":"${upd.name.get}","backendType":"${upd.backendType}",""" +
            s""""description":"${upd.description.get}","params":{"${upd.params.toSeq.head._1}":"${upd.params.toSeq.head._2}"}}""")
    }

    "create Backend Config" in {
      val c = createCatalog()
      val bc = generateBackendConfigRow(c.catalogId)
      val create = route(
        app,
        FakeRequest(POST, s"/catalogs/${c.catalogId}/backendConfigs").withBody(BackendConfigReq.fromRow(bc))).get

      status(create) mustBe CREATED
      contentType(create) mustBe Some("application/json")
      val content = contentAsJson(create)
      val fields = new TableFor2(
        "fieldName" -> "value",
        Json
          .obj(
            "catalogId" -> c.catalogId,
            "name" -> bc.name,
            "backendType" -> bc.backendType,
            "description" -> bc.description.get,
            "params" -> JsObject(bc.params.mapValues(JsString(_)))
          )
          .value
          .toSeq: _*
      )

      forAll(fields)((k, v) => (content \ k) mustBe JsDefined(v))
    }

  }
}
