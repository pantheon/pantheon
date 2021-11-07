package pantheon.schema

import java.sql

import org.scalatest.{MustMatchers, WordSpec}
import org.scalatest.prop.TableDrivenPropertyChecks
import pantheon.schema.parser.FilterParser
import pantheon.planner.ASTBuilders._
import pantheon.planner.Predicate

class FilterParserSpec extends WordSpec with MustMatchers with TableDrivenPropertyChecks {

  "FilterParser" must {
    "correctly parse filter (with different quote contexts)" in {
      val timeStamp = sql.Timestamp.valueOf("2012-12-12 01:01:01")
      val date = sql.Date.valueOf("2012-12-12")

      val positiveCases = Table(
        ("Filter", "Expected"),
        s"""A >
          | 'X'""".stripMargin -> (ref("A") > lit("X")),
        s"""A = 'X'""" -> (ref("A") === lit("X")),
        s"""(A = 'X')""" -> (ref("A") === lit("X")),
        s"""((((((A = 'X'))))))""" -> (ref("A") === lit("X")),
        s"""A != 'X''X'""" -> (ref("A") !== lit("X'X")),
        s"""A >= true""" -> (ref("A") >= lit(true)),
        s"""A <= 1""" -> (ref("A") <= lit(1)),
        s"""A.B <= 1""" -> (ref("A.B") <= lit(1)),
        s"""A.15 = 1""" -> (ref("A.15") === lit(1)),
        s"""A like (2, 1)""" -> ref("A").like(lit(2), lit(1)),
        s"""A is null""" -> ref("A").isNull,
        s"""A is not null""" -> ref("A").isNotNull,
        s"""A in (1, 2)""" -> ref("A").in(lit(1), lit(2)),
        s"""A > date '$date'""" -> (ref("A") > lit(date)),
        s"""A > timestamp '$timeStamp'""" -> (ref("A") > lit(timeStamp)),
        // quoted dates/timestamps are strings
        s"""A > '$timeStamp'""" -> (ref("A") > lit(timeStamp.toString)),
        s"""N = 'X' or A = 'X' and B = 'X' or C='X' """ -> (
          (
            ref("N") === lit("X") |
              (
                ref("A") === lit("X") &
                ref("B") === lit("X")
              )
          ) |
            ref("C") === lit("X")
        ),
        s"""(N = 'X' or A = 'X') and B = 'X' or C='X' """ -> (
          (
           (
             ref("N") === lit("X") |
             ref("A") === lit("X")
           ) &
             ref("B") === lit("X")
          ) |
            ref("C") === lit("X")
        ),
        s"""Y = 'X' or (N = 'X' or A = 'X') and (B = 'X' or C='X') or X='X'  """ -> (
          (
            ref("Y") === lit("X") |
             (
               (ref("N") === lit("X") | ref("A") === lit("X")) &
               (ref("B") === lit("X") | ref("C") === lit("X"))
             )
         ) |
           ref("X") === lit("X")
        )
      )

      forAll(positiveCases) { (filterStr, result) =>
        FilterParser(filterStr) mustBe Right(result)
      }

      // negative cases:
      val negativeCases = Table(
        "filter" -> "error",
        s"""A""" -> "filter must contain at least one operation",
        s"""A = 2)""" -> "unmatched opening parenthesis is not detected",
        s"""(A = 2""" -> "unmatched closing parenthesis is not detected",
        s"""A in (1, null)""" -> "in null must not be supported",
        s"""A in (null)""" -> "in null must not be supported",
        s"""A not in (1, null)""" -> "not in null must not be supported",
        s"""A not in (null)""" -> "not in null must not be supported",
        s"""A like (null)""" -> "like null must not be supported",
        s"""A == null""" -> "== null must not be supported",
        s"""A > "X" """ -> "double quotes inside filter",
        s"""A > date 'X' """ -> "malformed date",
        s"""A > timestamp 'X' """ -> "malformed timestamp",
        s"""A. > 5""" -> "invalid identifier",
        s"""A.5. > 5""" -> "invalid identifier",
        s"""A in (1, 'X') """ -> "list of different types"
      )
      forAll(negativeCases) {
        case (filterStr, msg) => assert(FilterParser(filterStr).isLeft, msg)
      }

    }
    //ANTLR is not stack safe. StackOverflow at :org.antlr.v4.runtime.RuleContext.getText(RuleContext.java:131)
    "be stack safe"  ignore {

      def generateFilter(depth: Int): Predicate =
        (1 to depth).foldLeft[Predicate](ref("x") > lit(3))((v, _) => v & v)

      FilterParser(SchemaPrinter.printExpression(generateFilter(10000))) mustBe 'right
    }
  }
}
