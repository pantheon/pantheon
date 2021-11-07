package util

import cats.Applicative
import cats.kernel.Monoid
import com.fasterxml.jackson.core.JsonParseException
import play.api.libs.json._

import scala.language.higherKinds

object JsonUtils {

  //Moves branch applying transformation
  def move[B: Format](from: JsPath)(f: B => B, to: JsPath = from): Reads[JsObject] = {
    from.read[B].flatMap(b => __.read[JsObject].map(_.deepMerge(to.write[B].writes(f(b)))))
  }

  //shallow for non-deep merge
  implicit val jsObjectShallowMonoid = new Monoid[JsObject] {
    override def empty = Json.obj()
    override def combine(x: JsObject, y: JsObject) = x ++ y
  }

  implicit val jsResultApplicative: Applicative[JsResult] = new Applicative[JsResult] {
    override def pure[A](x: A) = JsSuccess(x)
    override def ap[A, B](ff: JsResult[A => B])(fa: JsResult[A]) = ff.flatMap(f => fa.map(f))
  }

  implicit class JsResultOps[T](e: JsResult[T]) {
    def mapErrMsgs(f: String => String): JsResult[T] = e match {
      case JsError(errs) =>
        JsError(
          errs.map { case (p, errs) => p -> errs.map(e => JsonValidationError(e.messages.map(f))) }
        )
      case x => x
    }
  }

  def safeParse(s: String): Either[String, JsValue] =
    try { Right(Json.parse(s)) } catch {
      case e: JsonParseException => Left(e.getMessage)
//      case e: MismatchedInputException => Left("cannot parse empty input")
    }

}
