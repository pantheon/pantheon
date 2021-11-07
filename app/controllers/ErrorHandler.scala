package controllers

import com.typesafe.scalalogging.StrictLogging
import play.api.http.DefaultHttpErrorHandler
import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.api.routing.Router
import play.core.SourceMapper
import Writables.jsonWritable

import scala.concurrent._

// covers errors that happen before entering the controller (all other errors are handled in controllers or in handler transformation chain)
class ErrorHandler(
    env: Environment,
    config: Configuration,
    sourceMapper: Option[SourceMapper],
    router: => Router
) extends DefaultHttpErrorHandler(env, config, sourceMapper, Some(router))
    with StrictLogging {

  override def onForbidden(request: RequestHeader, message: String) = {
    logger.error(s"ForbiddenError at '${request.uri}' : $message")
    Future.successful(
      Forbidden(ActionError("You're not allowed to access this resource."))
    )
  }

  override def onClientError(request: RequestHeader, statusCode: Int, message: String) = {
    logger.error(s"ClientError $statusCode at '${request.uri}' $message")
    Future.successful {
      val status = Status(statusCode)
      if (message.isEmpty) status else status(ActionError(message))
    }
  }

//  override def onServerError(request: RequestHeader, exception: Throwable) =
//    throw new AssertionError(
//      s"All server errors must be handled by PantheonActionBuilder(play default action builder must not be used)" +
//        s"but encountered one in HttpErrorHandler : $exception")
//
}
