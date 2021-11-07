package config

import controllers.ActionError
import play.api.BuiltInComponents
import play.api.http.FileMimeTypes
import play.api.i18n.{Langs, MessagesApi}
import play.api.libs.json._
import play.api.mvc._
import cats.data.NonEmptyList.fromListUnsafe
import scala.concurrent.ExecutionContext
import controllers.Writables.jsonWritable

class PantheonBodyParsers(c: BuiltInComponents)
    extends DefaultPlayBodyParsers(c.httpConfiguration.parser, c.httpErrorHandler, c.materializer, c.tempFileCreator) {

  override def json[T](implicit r: Reads[T]): BodyParser[T] = {
    BodyParser("pantheon json reader") { request =>
      json(request).map {
        case Left(simpleResult) => Left(simpleResult)
        case Right(jsValue) =>
          r.reads(jsValue).asEither.left.map(errs => Results.UnprocessableEntity(ActionError.fromJsErrorsUnsafe(errs)))
        //TODO: use same thread execution context
      }(c.executionContext)
    }
  }
}

class PantheonControllerComponents(c: BuiltInComponents) extends ControllerComponents {

  override def actionBuilder: ActionBuilder[Request, AnyContent] =
    throw new AssertionError("Default action builder must not be used")

  override def parsers: PlayBodyParsers = new PantheonBodyParsers(c)

  def messagesApi: MessagesApi = c.messagesApi
  def executionContext: ExecutionContext = c.executionContext
  def fileMimeTypes: FileMimeTypes = c.fileMimeTypes
  def langs: Langs = c.langs
}
