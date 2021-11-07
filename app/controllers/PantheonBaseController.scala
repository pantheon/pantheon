package controllers

import play.api.mvc.{BaseController, ControllerComponents}

class PantheonBaseController(val controllerComponents: ControllerComponents) extends BaseController {
  implicit lazy val ec = defaultExecutionContext
}
