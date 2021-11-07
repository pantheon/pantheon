package controllers.data

import pantheon.planner._
import pantheon.planner.literal.Literal
import util.JsonUtils._
import util.JsonSerializers._
import play.api.libs.json._

package object serialization {

  private type LitType = String

  def convertNumber(v: BigDecimal): Literal = {
    if (v.isValidInt) literal.Integer(v.intValue())
    else if (v.isValidLong) literal.Long(v.longValue())
    else literal.Decimal(v)
  }

  /**
    * Reads literal given the optional type.
    * If type label is defined:
    * use play generated reads for ADT
    * Else
    * infer Literal types form JsValue subtypes (JsString => String, JsNumber => Number, JsBoolean => Boolean)
    */
  implicit val literalReadsWithOptionalType: Reads[Literal] = {
    val untypedReads: Reads[Literal] = {
      case JsString(s)              => JsSuccess(literal.String(s))
      case JsNumber(v)              => JsSuccess(convertNumber(v))
      case JsBoolean(v)             => JsSuccess(literal.Boolean(v))
      case JsNull                   => JsSuccess(literal.Null)
      case _: JsArray | _: JsObject => JsError("This message should not be dislayed. Switching to adtReads")
    }

    untypedReads.orElse(adtReads[Literal])
  }
}
