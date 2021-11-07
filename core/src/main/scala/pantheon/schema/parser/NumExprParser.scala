package pantheon.schema.parser

import pantheon.planner._
import pantheon.schema.parser.grammar.{NumericExpressionLexer, NumericExpressionParser}
import pantheon.schema.parser.ParserUtils._
import pantheon.schema.parser.grammar.NumericExpressionParser._
import pantheon.planner.literal.{Decimal, Integer}
import pantheon.util.Tap

object NumExprParser {
  def apply(expr: String): Either[String, NumericExpressionAST] =
    parse(new NumericExpressionParser(_), new NumericExpressionLexer(_))(expr,
                                                                         parser => readBody(parser.expression().body()))

  private def readBody(ctx: BodyContext): NumericExpressionAST = {

    def readNumber(numStr: String): NumericExpressionAST =
      if (numStr.contains('.')) Decimal(BigDecimal(numStr))
      else Integer(numStr.toInt)

    def readOperation: (NumericExpressionAST, NumericExpressionAST) => NumericExpressionAST =
      oneOf("operation")(
        ctx.PLUS.?|>(_ => PlusAST),
        ctx.MINUS() ?|> (_ => MinusAST),
        ctx.DIVIDE() ?|> (_ => DivideAST),
        ctx.MULTIPLY() ?|> (_ => MultiplyAST)
      )

    oneOf("body")(
      ctx.NUMBER() ?|> (n => readNumber(n.getText)),
      ctx.identWithDot() ?|> (i => UnresolvedField(i.getText)),
      ctx.body() ?|>> (read2(_).map { case (l, r) => readOperation(readBody(l), readBody(r)) }),
      ctx.body() ?|>> (read1(_).map(readBody))
    )
  }
}
