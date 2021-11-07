package controllers

import java.util.UUID

import cats.Show
import errors.{NotAuthorized, PantheonError}
import play.api.libs.json.{Json, Reads, Writes}
import play.api.mvc.Result
import services.Authorization._
import util.JsonSerializers.readsWithRandomDefaultIds

import scala.concurrent.Future

object AuthorizationControllerCommons {
  type ResourceId = UUID

  implicit val permReads: Reads[PermissionTO] = readsWithRandomDefaultIds(Json.reads[PermissionTO], "id")
  implicit val permWrites: Writes[PermissionTO] = Json.writes[PermissionTO]
  implicit val permKeyShow: Show[(ResourceType, ResourceId, ResourceId)] =
    Show.show[(ResourceType, ResourceId, UUID)] {
      case (resType, resId, permId) => s"Permission $permId for ${resType.toString} '$resId'"
    }

  private val notAuthResult = handlePantheonErr(NotAuthorized)

  def chkAuthorized(b: Boolean): Either[Result, Unit] = Either.cond(b, (), notAuthResult)
  def ifAuthorized(r: => Future[Result])(b: Boolean): Future[Result] = if (b) r else Future.successful(notAuthResult)
  def ifAuthorizedOrErr[T](r: => Future[Either[PantheonError, T]])(b: Boolean): Future[Either[PantheonError, T]] =
    if (b) r
    else Future.successful(Left(NotAuthorized))
}
