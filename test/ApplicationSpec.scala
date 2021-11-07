import org.scalatestplus.play._
import play.api.test._
import play.api.test.Helpers._
import util.Fixtures

class ApplicationSpec extends Fixtures with BaseOneAppPerSuite {
  "Routes" should {

    "send 404 on a bad request" in {
      route(app, FakeRequest(GET, "/foo")).map(status(_)) mustBe Some(NOT_FOUND)
    }
  }

  "HomeController" should {

    "render the index page" in {
      val home = route(app, FakeRequest(GET, "/")).get

      status(home) mustBe OK
      contentType(home) mustBe Some("text/html")
      contentAsString(home) must include("Your new application is ready.")
    }
  }

}