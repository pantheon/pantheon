package pantheon.util.internal
import cats.instances.option._
import cats.instances.string._
import cats.syntax.foldable._

object urlBuilder {

  implicit class UrlBuilder(val s: String) extends AnyVal {
    def addOpt(separator: String, v: Option[String]): String = s + v.foldMap(separator + _)
    def addOptInt(separator: String, v: Option[Int]): String = addOpt(separator, v.map(_.toString))
    def addParams(urlSeparatror: String, paramSeparator: String, params: (String, Option[String])*) = {
      val definedParams = params.flatMap { case (lbl, valOpt) => valOpt.map(lbl + "=" + _) }
      if (definedParams.isEmpty) s
      else s + definedParams.mkString(urlSeparatror, paramSeparator, "")
    }
  }
}
