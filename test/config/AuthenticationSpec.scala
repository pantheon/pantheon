package config

import java.util.UUID

import org.scalatest.{MustMatchers, WordSpec}
import org.scalatest.OptionValues._
import org.scalatest.EitherValues._
import pdi.jwt.JwtJson
import play.api.libs.json.Json
import play.api.mvc.AnyContentAsEmpty
import play.api.test.{FakeHeaders, FakeRequest}
import play.api.test.Helpers._

class AuthenticationSpec extends WordSpec with MustMatchers {
  "Authentication" must {
    "parse valid principal" in {
      val (subId, cat1, cat2, group1, group2) =
        (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
      val claim = Json.obj(
        "exp" -> 1539951848,
        "iat" -> 1539951848,
        "nbf" -> 1539951848,
        "sub" -> subId,
        "name" -> "User",
        "isTenantAdmin" -> true,
        "realmIDs" -> Seq(cat1, cat2),
        "adminRealmIDs" -> Seq(cat1, cat2),
        "groupIDs" -> Seq(group1, group2)
      )
      val req =
        FakeRequest(GET, "/some/url", FakeHeaders(Seq("X-Request-Token" -> JwtJson.encode(claim))), AnyContentAsEmpty)

      val principal = Authentication.getUser(req).value.right.value
      principal.userId mustBe subId
      principal.name mustBe "User"
      principal.tenantAdmin mustBe true
      principal.catalogIds mustBe Seq(cat1, cat2)
      principal.adminCatalogIds mustBe Seq(cat1, cat2)
      principal.groupIds mustBe Seq(group1, group2)
    }

    "return None if JWT header is not passed" in {
      Authentication.getUser(FakeRequest(GET, "/some/url")) mustBe 'empty
    }

    "return error if JWT cannot be read" in {
      val req = FakeRequest(GET, "/some/url", FakeHeaders(Seq("X-Request-Token" -> "garbage")), AnyContentAsEmpty)

      Authentication
        .getUser(req)
        .value
        .left
        .value mustBe "pdi.jwt.exceptions.JwtLengthException: Expected token [garbage] to be composed of 2 or 3 parts separated by dots."
    }

    "return error if principal cannot be read from JSON" in {
      val (subId, uuid1, uuid2) = (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
      val claim = Json.obj(
        "exp" -> 1539951848,
        "iat" -> 1539951848,
        "nbf" -> 1539951848,
        "sub" -> subId,
        "name" -> "User",
        "realmIDs" -> Seq(uuid1, uuid2),
        "adminRealmIDs" -> Seq(uuid1, uuid2),
        "groupIDs" -> Seq(uuid1, uuid2)
      )

      val req =
        FakeRequest(GET, "/some/url", FakeHeaders(Seq("X-Request-Token" -> JwtJson.encode(claim))), AnyContentAsEmpty)

      Authentication
        .getUser(req)
        .value
        .left
        .value mustBe "List((/isTenantAdmin,List(JsonValidationError(List(error.path.missing),WrappedArray()))))"
    }
  }
}
