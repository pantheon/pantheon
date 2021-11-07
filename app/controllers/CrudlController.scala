package controllers

import java.util.UUID

import cats.Show
import cats.data.EitherT
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons._
import controllers.Writables.jsonWritable
import controllers.helpers.CrudlActions
import pantheon.util.ConcreteChild
import play.api.libs.json.Writes
import play.api.mvc.{ControllerComponents, Result}
import services.{Authorization, CatalogRepo}
import services.Authorization.{ActionType, PermissionTO}
import services.CatalogRepo.CatalogId

import scala.language.higherKinds
import scala.concurrent.Future

/*
 * The instance of ConcreteChild[Opt[_], Option[_]] guarantees that Opt is known to be Some or None in compile time.
 * This allows to make sure that if calatogId is used than it must be provided everywhere and chkCatalogExists also must be provided.
 * Otherwise both must not be provided.
 */
class CrudlController[Res, Opt[x] <: Option[x]](
    components: ControllerComponents,
    action: PantheonActionBuilder,
    auth: Authorization,
    resourceType: Authorization.ResourceType,
    chkCatalogExists: Opt[UUID => EitherT[Future, Result, Unit]],
    extractId: Res => ResourceId)(implicit wr: Writes[Res], c: ConcreteChild[Opt[_], Option[_]])
    extends PantheonBaseController(components) {

  val actions = new CrudlActions[Res, Opt](auth, resourceType, extractId, chkCatalogExists)

  // TODO: need to use Opt on permissions endpoints too, but currently Play's routes limitations are standing in a way(in case of None it will try to call classOf[None.type] and fail in compile time)
  implicit val resourceIdShow: Show[(ResourceId, UUID)] = {
    case (resId, permId) => s"$resourceType '$resId' for permission '$permId'"
  }

  def listPermissions(resId: ResourceId, catId: Option[CatalogId] = None) =
    action(
      request =>
        _ =>
          auth
            .checkPermission(request.user, resourceType, ActionType.Grant, catId, resId)
            .flatMap(ifAuthorized(auth.list(resId).map(Ok(_)))))

  def findPermission(resId: ResourceId, permId: UUID, catId: Option[CatalogId] = None) =
    action(
      request =>
        _ =>
          auth
            .checkPermission(request.user, resourceType, ActionType.Grant, catId, resId)
            .flatMap(ifAuthorized(auth.find(resId, permId).map(_.fold(idNotFound((resId, permId)))(Ok(_))))))

  def createPermission(resId: ResourceId, catId: Option[CatalogId] = None) =
    action(parse.json[PermissionTO]) { request => _ =>
      auth
        .checkPermission(request.user, resourceType, ActionType.Grant, catId, resId)
        .flatMap(
          ifAuthorized(auth.create(resId, resourceType, request.body).map(_.fold(handlePantheonErr, Created(_)))))
    }

  def updatePermission(resId: ResourceId, permissionId: UUID, catId: Option[CatalogId] = None) =
    action(parse.json[PermissionTO]) { request => _ =>
      auth
        .checkPermission(request.user, resourceType, ActionType.Grant, catId, resId)
        .flatMap(
          ifAuthorized(
            auth
              .update(resId, resourceType, request.body.copy(id = permissionId))
              .map(_.fold(handlePantheonErr,
                          _.fold(NotFound(ActionError(s"permissionId $permissionId not found")))(Ok(_))))))

    }

  def deletePermission(resId: ResourceId, permId: UUID, catId: Option[CatalogId] = None) = action { request => _ =>
    auth
      .checkPermission(request.user, resourceType, ActionType.Grant, catId, resId)
      .flatMap(
        ifAuthorized(
          auth
            .delete(resId, permId)
            .map(
              _.fold(
                handlePantheonErr,
                deleted =>
                  if (deleted) Ok
                  else idNotFound((resId, permId))
              ))
        )
      )
  }
}
