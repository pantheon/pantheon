package controllers

import config.PantheonActionBuilder
import play.api.mvc._

import scala.concurrent.Future

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
class HomeController(components: ControllerComponents, action: PantheonActionBuilder)
    extends AbstractController(components) {

  /**
    * Create an Action to render an HTML page with a welcome message.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */
  def index = action {
    Future.successful(Ok(views.html.index("Your new application is ready.")))
  }

}
