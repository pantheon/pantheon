package pantheon

package object schema {
  // TODO: remove type parameter( and rename to MetaDataValue?)
  sealed trait Value[T] { def v: T }
  sealed trait PrimitiveValue[T] extends Value[T]
  case class BooleanValue(v: Boolean) extends PrimitiveValue[Boolean]
  case class StringValue(v: String) extends PrimitiveValue[String]
  case class NumericValue(v: BigDecimal) extends PrimitiveValue[BigDecimal]
  case class ArrayValue(v: List[PrimitiveValue[_]]) extends Value[List[PrimitiveValue[_]]]

  type ValueMap = Map[String, Value[_]]

  def mkName(nameComponents: String*): String = nameComponents.filterNot(_.isEmpty).mkString(".")

}
