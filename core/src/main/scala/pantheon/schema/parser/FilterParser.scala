package pantheon.schema.parser

import java.sql.{Date, Timestamp}

import org.antlr.v4.runtime.tree.TerminalNode
import pantheon.planner._
import pantheon.planner.literal.Literal
import pantheon.schema.parser.ParserUtils._
import pantheon.util.Tap
import pantheon.schema.parser.grammar.PslFilterParser.{FilterContext, QueryParamContext, OpContext, ExprContext}
import cats.Eval
import pantheon.schema.parser.grammar.{PslFilterLexer, PslFilterParser}
import pantheon.util.BoolOps
import cats.syntax.apply._

import scala.collection.JavaConverters._

object FilterParser {

  def apply(filter: String): Either[String, Predicate] =
    parse(new PslFilterParser(_), new PslFilterLexer(_))(filter, parser => readFilter(parser.filter()))

  private def convertNumber(v: BigDecimal): Literal = {
    if (v.isValidInt) literal.Integer(v.intValue())
    else if (v.isValidLong) literal.Long(v.longValue())
    else literal.Decimal(v)
  }

  private def readFilter(ctx: FilterContext): Predicate = {

    def cleanDates(s: String, prefix: String) = s.replaceFirst(s"^$prefix\\s+", "").replaceAll("'", "")

    def asBoolean(n: TerminalNode) = literal.Boolean(n.getText.toBoolean)

    def asString(n: TerminalNode) = literal.String(n.unquote.replaceAll("''", "'"))

    def asNumber(n: TerminalNode) = convertNumber(BigDecimal(n.getText))

    def asDate(n: TerminalNode) = literal.Date(Date.valueOf(cleanDates(n.getText, "date")))

    def asTimeStamp(n: TerminalNode) = literal.Timestamp(Timestamp.valueOf(cleanDates(n.getText, "timestamp")))

    def extractName(qp: QueryParamContext): String = qp.LETTER_CHARS().getText

    def parseOp(_op: OpContext): Predicate = {

      val lOperand = UnresolvedField(_op.identifier().getText)

      oneOfOpt(s"Operation", _op)(
        _.unaryOperation() ?|> {
          oneOfOpt(s"Unary operation", _)(
            _.IsNotNull() ?|> (_ => IsNotNull(lOperand)),
            _.IsNull() ?|> (_ => IsNull(lOperand))
          )
        },
        _.binaryOperation() ?|> { binOp =>
          val rOperand = oneOfOpt(s"Value", binOp.value())(
            _.queryParam() ?|> (qp => BindVariable(extractName(qp))),
            _.BOOL() ?|> asBoolean,
            _.NUMBER() ?|> asNumber,
            _.SINGLEQUOTEDSTRINGLIT() ?|> asString,
            _.DATE() ?|> asDate,
            _.TIMESTAMP() ?|> asTimeStamp
          )

          val operation = oneOfOpt(s"Binary operator", binOp.binaryOperator())(
            _.EQ() ?|> (_ => EqualTo(_, _)),
            _.NEQ() ?|> (_ => NotEqual(_, _)),
            _.GT() ?|> (_ => GreaterThan(_, _)),
            _.LT() ?|> (_ => LessThan(_, _)),
            _.GTEQ() ?|> (_ => GreaterThanOrEqual(_, _)),
            _.LTEQ() ?|> (_ => LessThanOrEqual(_, _))
          )

          operation(lOperand, rOperand)
        },
        _.multivalOperation() ?|> { op =>
          val rOperand = oneOfOpt("Multival operand", op.multival())(
            _.multiBool() ?|> (_.BOOL().asScala.map(asBoolean)),
            _.multiString() ?|> (_.SINGLEQUOTEDSTRINGLIT().asScala.map(asString)),
            _.multiNumber() ?|> (_.NUMBER().asScala.map(asNumber)),
            _.multiDate() ?|> (_.DATE().asScala.map(asDate)),
            _.multiTimeStamp() ?|> (_.TIMESTAMP().asScala.map(asTimeStamp))
          )

          val operation = oneOfOpt("Multival operation", op.multivalOperator())(
            _.IN() ?|> (_ => In(_, _)),
            _.NOTIN() ?|> (_ => NotIn(_, _)),
            _.LIKE() ?|> (_ => Like(_, _))
          )

          operation(lOperand, rOperand)
        }
      )
    }

    def parseExpr(exprCtx: ExprContext): Predicate =
      oneOfOpt("expr", exprCtx)(
        _.op() ?|> (op => parseOp(op)),
        _.expr() ?|>> read1 map parseExpr,
        _.expr() ?|>> read2 map {
          case (l, r) =>
            oneOfOpt("And/Or", exprCtx)(
              _.AND() ?|> (_ => And(parseExpr(l), parseExpr(r))),
              _.OR() ?|> (_ => Or(parseExpr(l), parseExpr(r)))
            )
        }
      )

    parseExpr(ctx.expr())
  }
}
