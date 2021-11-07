package controllers

import buildinfo.BuildInfo
import controllers.StatusController.StatusResponse
import play.api.libs.json.Json
import controllers.Writables._
import config.PantheonActionBuilder
import play.api.mvc.{BaseController, ControllerComponents}

import scala.concurrent.Future
import config.PantheonActionBuilder

object StatusController {
  case class StatusResponse(version: String)
  implicit val statusRespWrites = Json.writes[StatusResponse]
}
class StatusController(val controllerComponents: ControllerComponents, action: PantheonActionBuilder)
    extends BaseController {
  def getStatus = action(Future.successful(Ok(StatusResponse(BuildInfo.version))))
}
