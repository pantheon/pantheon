package pantheon.schema.parser

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import java.{util => javautil}

import pantheon.util.{BoolOps, Tap}

import scala.collection.JavaConverters._
import scala.reflect.internal.util.Collections.distinctBy

object ParserUtils {

  /**
    * Reads collection from nullable object.
    * @param t "Base" object
    * @param f Function to get Java list from "base" object
    * @tparam T Type of "base" object
    * @tparam R Type of resulting collection element
    * @return Empty collection If "base" object us null else apply function to "base" object and convert to Scala
    */
  def readSeq[T, R](t: T)(f: T => javautil.List[R]): Seq[R] = if (t == null) Nil else f(t).asScala

  def readNel[T](l: javautil.List[T]): Option[List[T]] = (l.size() > 0).option(l.asScala.toList)
  def read1[T](l: javautil.List[T]): Option[T] = (l.size() == 1).option(l.get(0))
  def read2[T](l: javautil.List[T]): Option[(T, T)] = (l.size() == 2).option(l.get(0) -> l.get(1))
  def readN[T](size: Int, l: javautil.List[T]): Option[Vector[T]] = (l.size() == size).option(l.asScala.toVector)

  /**
    * Ensure exactly one alternative and return this alternative.
    * @param label Label describing target value
    * @param t "Base" object
    * @param alternatives Collection of functions from "base" object to nullable value
    * @tparam T "Base" object type
    * @tparam R Type of the result
    * @return One and only value among alternatives
    */
  def oneOf[T <: ParserRuleContext, R](label: String, t: T)(alternatives: (T => R)*): R =
    oneOf(label + s":'${t.getText}'")(alternatives.map(ff => Option(ff(t))): _*)

  /**
    * Ensure exactly one alternative and return this alternative.
    * @param label Label describing target value
    * @param t "Base" object
    * @param alternatives Collection of functions from "base" object to optional value
    * @tparam T "Base" object type
    * @tparam R Type of the result
    * @return One and only value among alternatives
    */
  def oneOfOpt[T <: ParserRuleContext, R](label: String, t: T)(alternatives: (T => Option[R])*): R =
    oneOf(label + s":'${t.getText}'")(alternatives.map(_(t)): _*)

  /**
    * Ensure exactly one alternative and return this alternative.
    * @param label Label describing target value
    * @param alternatives Collection of optional values
    * @tparam R The type of the value
    * @return One and only value among alternatives
    */
  def oneOf[R](label: String)(alternatives: Option[R]*): R = {
    val alts = alternatives.flatten
    assert(alts.nonEmpty, s"No provided alternatives at $label")
    assert(alts.size == 1, s"Multiple alternatives found at $label")
    alts.head
  }

  /**
    * Captures nullable value and an function on it, so it can be continued after inspection of the
    * saved value (like the notnull check).
    * This class in covariant in T and R(covariance needed when collecting lists of continuations).
    * This cannot be expressed with just 'v' and 'f' because of covariance of T.
    * @param value Saved value
    * @param f Continuation function from saved value to resulting type
    * @tparam T Type of the saved value
    * @tparam R Resulting type
    */
  final class Continuation[+T, +R](val value: T, f: T => R) {

    /**
      * Applies continuation function on saved value
      */
    val continue: () => R = () => f(value)

    /**
      * Creates new Continuation with continuation function composed of given function applied after saved function
      * @param f1 Function to apply after saved continuation function
      * @tparam X New continuation result type
      * @return New continuation with composed function
      */
    def map[X](f1: R => X): Continuation[T, X] = new Continuation(value, f.andThen(f1))
  }

  implicit class ContSyntax[T](val t: T) extends AnyVal {
    def -->[R](f: T => R): Continuation[T, R] = new Continuation[T, R](t, f)
  }

  /**
    * Validates that there are no duplicate parameters
    * and returns the lookup map where params indexed by order of alternatives functions.
    * @param label Label for parameters context
    * @param params Parameter values
    * @param alternatives Functions from parameter value type to continuation
    * @tparam P type of the parameter
    * @return Either error or parameters lookup map
    */
  def readStringParams[P <: ParserRuleContext](label: String, params: Seq[P])(
      alternatives: (P => Continuation[ParserRuleContext, TerminalNode])*
  ): Either[String, List[Option[String]]] = {
    readParams(label, params)(alternatives.map(_.andThen(_.map(_.unquote))): _*)
  }

  /**
    * Validates that there are no duplicate parameters
    * and returns the lookup map where params indexed by order of alternatives functions.
    * @param label Label for parameters context
    * @param params Parameter values
    * @param alternatives Functions from parameter value type to continuation
    * @tparam P type of the parameter
    * @tparam R result parameter type after applying continuation
    * @return Either error or parameters lookup map
    */
  def readParams[P <: ParserRuleContext, R](label: String, params: Seq[P])(
      alternatives: (P => Continuation[ParserRuleContext, R])*
  ): Either[String, List[Option[R]]] = {

    case class ParamVal(altIndex: Int, param: R)

    val paramVals: List[ParamVal] = params.map(
      p =>
        oneOf(s"$label / parameter: ${p.getText}")(
          alternatives.zipWithIndex.map {
            case (contF, i) =>
              val alternative = contF(p)
              (alternative.value != null).option(ParamVal(i, alternative.continue()))
          }: _*
      )
    )(collection.breakOut)

    Either.cond(
      distinctBy(paramVals)(_.altIndex).size == params.size,
      (0 until alternatives.size).map(i => paramVals.find(_.altIndex == i).map(_.param))(collection.breakOut),
      s"duplicated parameter types at $label"
    )
  }

  implicit class TnOps(val v: TerminalNode) extends AnyVal {
    def unquote: String = v.getText.replaceAll("(^\"+)|(\"+$)|(^')|('$)", "")
  }

  def parse[P <: Parser, L <: TokenSource, R](mkParser: CommonTokenStream => P, mkLexer: CharStream => L)(
      input: String,
      read: P => R): Either[String, R] = {
    try {
      Right(
        read(
          mkParser(new CommonTokenStream(mkLexer(CharStreams.fromString(input))))
            .tap(_.addErrorListener(ErrListener))
        )
      )
    } catch {
      case re: SyntaxError => Left(re.getMessage)
    }
  }
}
