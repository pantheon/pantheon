package pantheon.schema

import org.scalatest.{Matchers, MustMatchers, WordSpec}
import org.scalatest.prop.TableDrivenPropertyChecks
import pantheon.planner.ASTBuilders.{NumericExprBuilder, lit, ref}
import pantheon.schema.parser.NumExprParser
import TableDrivenPropertyChecks._

class NumericExpressionParserSpec extends WordSpec with MustMatchers with TableDrivenPropertyChecks {

  "NumericExpressionParser" must {
    "parse expressions respecting priority of operations and parentheses" in {
      val table = Table(
        "Expression" -> "result",
        "-1.2" -> lit(BigDecimal(-1.2)),
        "(1)" -> lit(1),
        "Parent.child + 2 - 3 + 4.3" -> (ref("Parent.child") + lit(2) - lit(3) + lit(BigDecimal(4.3))),
        "1 * 2 / 3 / 4" -> (lit(1) * lit(2) / lit(3) / lit(4)),
        "(5 / (-1 * x - 3) + 4) * 2.1 + 5" ->
          ((lit(5) / (lit(-1) * ref("x") - lit(3)) + lit(4)) * lit(BigDecimal(2.1)) + lit(5))
      )

      forAll(table)((exprStr, res) => NumExprParser(exprStr) mustBe Right(res))
    }
  }
}
