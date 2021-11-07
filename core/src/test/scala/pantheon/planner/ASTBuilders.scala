package pantheon.planner

import java.sql

import literal.Literal
import pantheon.util.{=:!=, ConcreteChild}

import scala.language.implicitConversions

// This may be exposed as Predicate building interface for the users
object ASTBuilders {

  // basically this is the controlled implicit conversion
  trait LiteralFor[T] {
    type Out
    def apply(t: T): Out
  }

  object LiteralFor {
    class LitFor[T, L](f: T => L) extends LiteralFor[T] {
      type Out = L
      override def apply(t: T): L = f(t)
    }
    // local conversion is ok(removing a bit of boilerplate)
    private implicit def liftToOption[T](t: T): Option[T] = Some(t)

    implicit val date = new LitFor[sql.Date, literal.Date](literal.Date)
    implicit val timestamp = new LitFor[sql.Timestamp, literal.Timestamp](literal.Timestamp)
    implicit val string = new LitFor[String, literal.String](literal.String)
    implicit val Boolean = new LitFor[Boolean, literal.Boolean](literal.Boolean)

    implicit val numI = new LitFor[Int, literal.Integer](literal.Integer)
    implicit val numL = new LitFor[Long, literal.Long](literal.Long)
    implicit def numF = new LitFor[Float, literal.Double](v => literal.Double(v.toDouble))
    implicit val numD = new LitFor[Double, literal.Double](literal.Double)
    implicit def numBD = new LitFor[BigDecimal, literal.Decimal](literal.Decimal)
  }

  def lit[T](v: T)(implicit lf: LiteralFor[T]): lf.Out = lf(v)
  def ref(v: String) = UnresolvedField(v)
  def bindVar(v: String) = BindVariable(v)

  type ConcreteLiteral[L] = ConcreteChild[L, Literal]
  //Can we apply these ops on somethig other than Literal, Measure, Dimension?
  implicit class PredicateBuilder[L <: Expression](val left: L) extends AnyVal {
    def ===(right: Expression) = EqualTo(left, right)
    def isNull = IsNull(left)
    def isNotNull = IsNotNull(left)
    def !==(right: Expression) = NotEqual(left, right)
    def >(right: Expression) = GreaterThan(left, right)
    def >=(right: Expression) = GreaterThanOrEqual(left, right)
    def <(right: Expression) = LessThan(left, right)
    def <=(right: Expression) = LessThanOrEqual(left, right)
    def &(right: Predicate)(implicit ev: L <:< Predicate) = And(left, right)
    def |(right: Predicate)(implicit ev: L <:< Predicate) = Or(left, right)
    // lets start with proper support of the simplest (yet most common) case of allowing only Literals in 'in', 'notIn', 'like expressions
    def in[T <: Literal: ConcreteLiteral](right: T*) = In(left, right)
    def notIn[T <: Literal: ConcreteLiteral](right: T*) = NotIn(left, right)
    def like[T <: Literal: ConcreteLiteral](right: T*) = Like(left, right)
  }

  implicit class NumericExprBuilder[L <: NumericExpressionAST](val left: L) extends AnyVal {
    def +(right: NumericExpressionAST) = PlusAST(left, right)
    def -(right: NumericExpressionAST) = MinusAST(left, right)
    def *(right: NumericExpressionAST) = MultiplyAST(left, right)
    def /(right: NumericExpressionAST) = DivideAST(left, right)
  }

}
