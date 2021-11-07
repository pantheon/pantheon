package config

import com.typesafe.scalalogging.Logger
import config.Authentication.{AuthenticatedRequest, getUser}
import controllers.ActionError
import pantheon.util.Logging
import pantheon.util.Logging.LoggingContext
import play.api.mvc._
import play.api.mvc.Results.{BadRequest, InternalServerError, Unauthorized}
import controllers.Writables.jsonWritable
import services.Tracing.TraceStarter

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class PantheonActionBuilder(playAction: ActionBuilder[Request, AnyContent],
                            authenticationEnabled: Boolean,
                            traceStarter: Option[TraceStarter])(implicit ec: ExecutionContext) {

  private type ActionF[A] = Request[A] => Future[Result]
  private type LoggedActionF[A] = Request[A] => LoggingContext => Future[Result]
  private type AuthenticatedActionF[A] = AuthenticatedRequest[A] => LoggingContext => Future[Result]
  private type LoggedActionT[A] = LoggedActionF[A] => LoggedActionF[A]

  def apply[A](bp: BodyParser[A]): AuthenticatedActionF[A] => Action[A] =
    pantheonActionStack[A].andThen(playAction(bp).async)

  def apply(f: AuthenticatedActionF[AnyContent]): Action[AnyContent] =
    apply(playAction.parser)(f)

  def apply(f: => Future[Result]): Action[AnyContent] =
    apply(playAction.parser)(_ => _ => f)

  private def pantheonActionStack[A]: AuthenticatedActionF[A] => ActionF[A] =
    // each transformation in chain 'wraps' next one (may make impact before and after it)
    genCtx[A]
      .compose(logTime)
      .compose(handleServerError)
      .compose(authenticate)

  private def genCtx[A]: LoggedActionF[A] => ActionF[A] =
    next =>
      r => {
        import play.api.routing.Router.RequestImplicits._
        val ctx = traceStarter
          .map { ts =>
            val ctxName = r.request.handlerDef.map(h => s"${h.controller}#${h.method}").getOrElse("#noHandler")
            Logging.newContextWithTracing(ctxName, ts)
          }
          .getOrElse(Logging.newContext)
        next(r)(ctx).andThen { case _ => ctx.finish() }
    }

  private val serverErrorLogger = Logger.takingImplicit[LoggingContext]("controllers.ServerErrorLogger")

  private def handleServerError[A]: LoggedActionT[A] =
    next =>
      lr =>
        implicit ctx => {
          def toInternalServerError(t: Throwable) = InternalServerError(ActionError.fromThrowable(t))
          try {
            next(lr)(ctx).recover {
              case t: Throwable =>
                serverErrorLogger.error("", t)
                toInternalServerError(t)

            }
          } catch {
            case NonFatal(t) =>
              serverErrorLogger.error("", t)
              Future.successful(toInternalServerError(t))
          }
    }

  private val requestLogger = Logger.takingImplicit[LoggingContext]("controllers.RequestLogger")

  private def logTime[A]: LoggedActionT[A] =
    next =>
      lr =>
        implicit ctx => {
          import play.api.routing.Router.RequestImplicits._
          val uri = lr.request.uri
          val methodName = lr.request.handlerDef
            .map { h =>
              s"${h.verb} $uri -> ${h.controller}#${h.method}"
            }
            .getOrElse(s"- $uri -> -#-")
          requestLogger.debug(methodName)

          val startTime = System.currentTimeMillis

          next(lr)(ctx)
            .map { result =>
              val elapsedTime = System.currentTimeMillis - startTime
              val resStatus = result.header.status
              requestLogger.debug(s"Response status: $resStatus completed in ${elapsedTime}ms")

              result
            }
    }

  private def authenticate[A]: AuthenticatedActionF[A] => LoggedActionF[A] =
    next =>
      lr =>
        implicit ctx => {
          if (authenticationEnabled) {
            getUser(lr) match {
              case None =>
                serverErrorLogger.error("No JWT token in request")
                Future.successful(Unauthorized(ActionError("No JWT token in request")))
              case Some(Left(e)) =>
                serverErrorLogger.error(s"Error reading JWT token: $e")
                Future.successful(BadRequest(ActionError(s"Error reading JWT token: $e")))
              case Some(Right(user)) =>
                next(new AuthenticatedRequest(user, lr))(ctx)
            }
          } else {
            next(new AuthenticatedRequest(Authentication.AnonymousUser, lr))(ctx)
          }
    }
}
