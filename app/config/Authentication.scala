package config

import java.util.UUID

import pdi.jwt.{JwtJson, JwtOptions}
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.mvc.{Request, RequestHeader, WrappedRequest}
import cats.syntax.either._

object Authentication {

  case class User(userId: UUID,
                  name: String,
                  tenantAdmin: Boolean,
                  catalogIds: Seq[UUID],
                  adminCatalogIds: Seq[UUID],
                  groupIds: Seq[UUID]) {
    def principalIds: Seq[UUID] = userId +: groupIds
  }

  // this user is used when authentication is switched off
  val AnonymousUser = User(UUID.randomUUID(), "Anonymous", false, Seq.empty, Seq.empty, Seq.empty)

  val userReads: Reads[User] = (
    (JsPath \ "sub").read[UUID] and
      (JsPath \ "name").read[String] and
      (JsPath \ "isTenantAdmin").read[Boolean] and
      (JsPath \ "realmIDs").read[Seq[UUID]] and
      (JsPath \ "adminRealmIDs").read[Seq[UUID]] and
      (JsPath \ "groupIDs").read[Seq[UUID]]
  )(User.apply _)

  val userWrites: OWrites[User] = (
    (JsPath \ "sub").write[UUID] and
      (JsPath \ "name").write[String] and
      (JsPath \ "isTenantAdmin").write[Boolean] and
      (JsPath \ "realmIDs").write[Seq[UUID]] and
      (JsPath \ "adminRealmIDs").write[Seq[UUID]] and
      (JsPath \ "groupIDs").write[Seq[UUID]]
  )(unlift(User.unapply))

  def getUser(request: RequestHeader): Option[Either[String, User]] = {
    request.headers.get("X-Request-Token").map { token =>
      Either
        .fromTry(JwtJson.decodeJson(token, JwtOptions(signature = false, expiration = false)))
        .leftMap(_.toString)
        .flatMap(_.validate[User](userReads).asEither.leftMap(_.toString))
    }
  }

  class AuthenticatedRequest[A](val user: User, request: Request[A]) extends WrappedRequest[A](request)
}
