package config

import org.scalatestplus.play.BaseOneAppPerSuite
import play.api.inject.DefaultApplicationLifecycle
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test._
import play.api.{ApplicationLoader, Configuration}
import play.core.DefaultWebCommands
import util.Fixtures

class PantheonActionBuilderSpec extends Fixtures with BaseOneAppPerSuite {
  override lazy val appContext = ApplicationLoader.Context(
    environment = env,
    sourceMapper = None,
    webCommands = new DefaultWebCommands(),
    initialConfiguration = Configuration.load(env) ++ Configuration("pantheon.auth.enabled" -> true),
    lifecycle = new DefaultApplicationLifecycle()
  )

  "Pantheon server with authentication enabled" should {
    "return response with valid JWT" in {
      val user = createUser()
      val list = route(app, FakeRequest(GET, "/catalogs").withUser(user)).get

      // check response
      status(list) mustBe OK
      contentType(list) mustBe Some("application/json")
      contentAsJson(list) mustBe Json.parse("""{"page":{"current":1,"itemsPerPage":2147483647,"itemCount":0},"data":[]}""")
    }

    "return Unauthorized with no JWT" in {
      val list = route(app, FakeRequest(GET, "/catalogs")).get

      // check response
      status(list) mustBe UNAUTHORIZED
      contentType(list) mustBe Some("application/json")
      contentAsString(list) mustBe """{"errors":["No JWT token in request"]}"""

    }

    "return BadRequest with invalid JWT" in {
      val list = route(app, FakeRequest(GET, "/catalogs").withHeaders("X-Request-Token" -> "invalidJWT")).get

      // check response
      status(list) mustBe BAD_REQUEST
      contentType(list) mustBe Some("application/json")
      contentAsString(list) mustBe """{"errors":["Error reading JWT token: pdi.jwt.exceptions.JwtLengthException: Expected token [invalidJWT] to be composed of 2 or 3 parts separated by dots."]}"""
    }
  }

}
