package controllers

import java.nio.file.Files
import java.util.UUID

import config.PantheonActionBuilder
import play.api.mvc.{ControllerComponents, Result}
import services.{Authorization, DataSourceProductRepo}
import AuthorizationControllerCommons.ifAuthorized
import errors.ConstraintViolation
import services.Authorization.{ActionType, ResourceType}

import scala.concurrent.Future

class DataSourceProductFileController(controllerComponents: ControllerComponents,
                                      action: PantheonActionBuilder,
                                      auth: Authorization,
                                      dspRepo: DataSourceProductRepo)
    extends PantheonBaseController(controllerComponents) {

  def getIcon(prodId: UUID) =
    action { request => _ =>
      auth
        .checkPermission(request.user, ResourceType.DataSourceProduct, ActionType.Read, None, prodId)
        .flatMap(ifAuthorized(dspRepo.getIcon(prodId).map(_.fold[Result](NotFound)(v => Ok(v).as("image/png")))))
    }

  def putIcon(prodId: UUID) =
    action(parse.multipartFormData(10 * (1 << 20)))(
      request =>
        _ =>
          auth
            .checkPermission(request.user, ResourceType.DataSourceProduct, ActionType.Edit, None, prodId)
            .flatMap(ifAuthorized(
              request.body.files match {
                case Seq(file) =>
                  dspRepo
                    .putIcon(prodId, Files.readAllBytes(file.ref.path))
                    .map(_ => Ok("Icon uploaded"))
                case _ =>
                  Future.successful(handlePantheonErr(ConstraintViolation("a single icon file must be provided")))
              }
            )))

  def put(prodId: UUID, name: String) =
    action(parse.byteString(10 * (1 << 20)))(
      request =>
        _ =>
          auth
            .checkPermission(request.user, ResourceType.DataSourceProduct, ActionType.Edit, None, prodId)
            .flatMap(
              ifAuthorized(dspRepo
                .putFile(prodId, name, request.body.toArray)
                .map(v => Ok("File uploaded")))))

  def delete(prodId: UUID, name: String) =
    action { request => _ =>
      auth
        .checkPermission(request.user, ResourceType.DataSourceProduct, ActionType.Edit, None, prodId)
        .flatMap(ifAuthorized(dspRepo.deleteFile(prodId, name).map(v => Ok(v.toString))))
    }

  def list(prodId: UUID) =
    action { request => _ =>
      auth
        .checkPermission(request.user, ResourceType.DataSourceProduct, ActionType.Read, None, prodId)
        .flatMap(ifAuthorized(dspRepo.listFiles(prodId).map(v => Ok(v.toString))))
    }
}
