package controllers

import cats.data.NonEmptyList
import org.scalatest.{MustMatchers, WordSpec}
import play.api.libs.json.{JsError, Json, Reads}

class ActionErrorSpec extends WordSpec with MustMatchers {
  case class B(j: Int)
  case class A(i0: Int, i: Int, s: String, b: Boolean, l: List[Int], l1: List[Int], lB: List[B], bb: B)
  implicit val bf = Json.format[B]
  implicit val af = Json.format[A]

  "ActionError" must {
    // yes, all-in-one testing is dirty, doing this to save time
    "be properly constructed  from play json errors and deserialized" in {

      def parseAndShowErrs[T: Reads](s: String) = {
        val r = Json.parse(s).validate[T]
        r.isInstanceOf[JsError] mustBe true
        Json.toJson(ActionError.fromJsErrorsUnsafe(r.asInstanceOf[JsError].errors))
      }

      parseAndShowErrs[A]("""{"i":true,"s":1,"b":"str","l":true, "l1":[3, "x"], "lB":[{"j":"x"}],"bb":{"j":"x"}}""") mustBe
        Json.parse("""{"fieldErrors":{
        |"s":["expected: string"],
        |"lB[0].j":["expected: number"],
        |"l1[1]":["expected: number"],
        |"b":["expected: boolean"],
        |"i":["expected: number"],
        |"i0":["is required"],
        |"bb.j":["expected: number"],
        |"l":["expected: array"]
        |}}""".stripMargin)

      parseAndShowErrs[Int]("\"x\"") mustBe (Json.parse("""{"errors":["expected: number"]}"""))

    }

    "be properly constructed from string and deserialized" in {
      Json.toJson(ActionError("foo")) mustBe (Json.parse("""{"errors":["foo"]}"""))
      Json.toJson(ActionError.fromList(NonEmptyList.of("foo", "bar"))) mustBe (Json.parse(
        """{"errors":["foo","bar"]}"""))
    }
  }
}
