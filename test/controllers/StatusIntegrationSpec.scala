package controllers

import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.test.FakeRequest
import play.api.test.Helpers.{GET, route}
import util.Fixtures
import play.api.test.Helpers._

class StatusIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  "Status controller" should {
    "general status of running app" in {
      val res = route(app, FakeRequest(GET, s"/status")).get
      (contentAsJson(res) \ "version").validate[String].isSuccess mustBe true
      status(res) mustBe 200
    }
  }
}
