import java.util.UUID

import cats.data.NonEmptyList.fromListUnsafe
import cats.data.NonEmptyList
import play.api.libs.json._
import play.api.mvc._
import play.api.mvc.Results.{BadRequest, UnprocessableEntity}
import cats.syntax.list._
import cats.syntax.show._

import scala.concurrent.{ExecutionContext, Future}
import _root_.util.JsonSerializers.nelFormat
import cats.Show
import controllers.Writables._
import errors.{ConcurrentModification, ConstraintViolation, NotAuthorized, PantheonError}
import pantheon.util.Logging.{ContextLogging, LoggingContext}
import play.api.libs.json.__
import pantheon.errors.QueryException
import services.|
import cats.syntax.either._
import cats.instances.future._

package object controllers extends ContextLogging {

  case class ActionError(errors: NonEmptyList[Error])

  sealed trait Error
  case class GeneralError(value: String) extends Error
  case class FieldError(fieldName: String, errors: NonEmptyList[String]) extends Error

  case class CustomRefs(queryId: Option[UUID] = None, customReference: Option[String] = None)

  implicit val customRefsReads: Reads[CustomRefs] = Json.reads[CustomRefs]

  implicit val stOrder = cats.kernel.Order.fromComparable[String]

  object NullableWritesOptionWrapper {
    val none: NullableWritesOptionWrapper[Nothing] = NullableWritesOptionWrapper(None)
    implicit def writes[T](implicit r: Writes[T]): Writes[NullableWritesOptionWrapper[T]] =
      _.value.map(Json.toJson(_)).getOrElse(JsNull)
    import scala.language.implicitConversions
    implicit def wrap[T](o: Option[T]): NullableWritesOptionWrapper[T] = NullableWritesOptionWrapper(o)
    implicit def unwrap[T](o: NullableWritesOptionWrapper[T]): Option[T] = o.value
  }
  // This class is only for usage in controller response-classes (this would not be needed if we used 'circe' instead of play-json)
  case class NullableWritesOptionWrapper[+T](value: Option[T])

  object ActionError {

    def fromThrowable(t: Throwable): ActionError =
      // TODO: adding the cause temporarily to simplify the debugging of issues.
      ActionError(t.getMessage + Option(t.getCause).fold("")(s" caused by " + _))
    def fromList(oe: NonEmptyList[String]): ActionError = ActionError(oe.map(GeneralError(_)))
    def apply(oe: String): ActionError = ActionError(NonEmptyList.one(GeneralError(oe)))
    // 'unsafe' prefix because relying on assumptions that sequences are not empty. Used translate between errors-from-JsError and ActionError
    def fromJsErrorsUnsafe(errs: Seq[(JsPath, Seq[JsonValidationError])]): ActionError = {

      def showError(err: JsonValidationError): String = {
        err.messages
          .map { msg =>
            msg
              .replace("error.path.missing", "is required")
              .replace("error.expected.js", "expected: ")
              .replace("error.expected.", "expected: ")

          }
          .mkString(";")
      }
      def showPath(p: JsPath) = p.toJsonString.stripPrefix("obj.")

      ActionError(
        NonEmptyList.fromListUnsafe[Error](
          errs.flatMap {
            case (`__`, jsErrs)   => jsErrs.map(e => GeneralError(showError(e)))
            case (jsPath, jsErrs) => Seq(FieldError(showPath(jsPath), fromListUnsafe(jsErrs.map(showError).toList)))
          }(collection.breakOut)
        )
      )
    }
  }

  implicit val errWrites: OWrites[ActionError] = v => {
    // not writing empty lists
    val errWrites = (__ \ "errors").writeNullable[NonEmptyList[String]]
    val fldErrWrites = (__ \ "fieldErrors").writeNullable[JsObject]

    val errors: List[String] = v.errors.collect { case GeneralError(e)       => e }
    val fldErrors: List[JsObject] = v.errors.collect { case FieldError(f, e) => Json.obj(f -> e) }

    errWrites.writes(errors.toNel) ++ fldErrWrites.writes(fldErrors.reduceLeftOption(_ ++ _))
  }

  def blocking[A](a: => A): Future[A] = Future.successful(concurrent.blocking(a))

  def idNotFound[ID: Show](id: ID): Result =
    Results.NotFound(ActionError(s"entity with id: '${id.show}' not found"))

  def logCustomRefs(r: CustomRefs)(implicit ctx: LoggingContext): Unit = {
    val reqInfo = r.queryId.map(id => s"queryId=$id") ++ r.customReference.map(ref => s"customReference=$ref")
    if (reqInfo.nonEmpty) logger.info(reqInfo.mkString(";"))
  }

  def handleQueryException[T](r: => Future[T])(implicit ec: ExecutionContext): Future[Result | T] = {
    def wrap(e: QueryException) = Left(UnprocessableEntity(ActionError(e.msg)))
    try {
      r.map(Right(_)).recover {
        case e: QueryException => wrap(e)
      }
    } catch {
      case e: QueryException => Future.successful(wrap(e))
    }
  }

  def handlePantheonErr(e: PantheonError): Result = e match {
    case NotAuthorized             => Results.Forbidden("Not authorized")
    case _: ConcurrentModification => Results.Conflict("concurrent modification detected, please try again")
    case ConstraintViolation(err)  => Results.UnprocessableEntity(ActionError(err))
  }

  type None[X] = None.type

  object ConnectionTestResult {
    def fromEither(e: Either[String, Boolean]): ConnectionTestResult = {
      e.fold(
        s => ConnectionTestResult(valid = false, Some(s)),
        b => ConnectionTestResult(b)
      )
    }
  }

  case class ConnectionTestResult(valid: Boolean, message: Option[String] = None)

  implicit val connectionTestResultFormat: Format[ConnectionTestResult] = Json.format[ConnectionTestResult]

  def validatePageParams[T](page: Option[Int], pageSize: Option[Int]): Either[String, Option[(Int, Int)]] =
    if (page.isDefined && pageSize.isDefined) {
      val (p, ps) = page.get -> pageSize.get
      Either.cond(p > 0 && ps > 0, Some(p -> ps), "page and pageSize must be positive")
    } else if (page.isEmpty && pageSize.isEmpty) Right(None)
    else Left("page and pageSize must be defined together")

  def validatePageParamsWithDefault[T](page: Option[Int], pageSize: Option[Int]): Either[Result, (Int, Int)] =
    controllers
      .validatePageParams(page, pageSize)
      .bimap(err => BadRequest(ActionError(err)), _.getOrElse(1 -> Int.MaxValue))

  def withPageParams(page: Option[Int], pageSize: Option[Int])(f: (Int, Int) => Future[Result])(
      implicit ec: ExecutionContext) = {
    validatePageParamsWithDefault(page, pageSize).traverse(f.tupled).map(_.merge)
  }
}
