package pantheon.schema

import cats.syntax.either._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import pantheon.planner.Predicate
import pantheon.planner.ASTBuilders._
import pantheon.schema.parser.SchemaParser
import org.scalatest.OptionValues._

class SchemaParserSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  val q3 = "\"\"\""
  "SchemaParser" when {
    "parsing schemas" should {
      "find error in invalid schema" in {
        val result = SchemaParser(
          """schema Sales (dataSource = "sales") {
            |
            |  dimension Date {
            |    level Year
            |    level Month
            |    level Date
            |  }
            | """.stripMargin
        )
        assert(result.isLeft)
      }

      "react on repeating parameter" in {
        val result = SchemaParser("""schema Sales (dataSource = "sales", dataSource = "sales2") {
            | """.stripMargin)
        assert(result.isLeft)
      }

      "disallow duplicate table names" in {
        val result = SchemaParser("""schema Sales  {
                                    |table t
                                    |table t
                                    } """.stripMargin)
        assert(result == Left("duplicate table names detected"))
      }

      "schema imports parsing properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  import { Inventory => i, GoogleAnalytics }
            |  import { FacebookStats => fb }
            |  import Foo._
            |  import Bar => Baz
            |}
          """.stripMargin
        )
        assert(result.isRight)
        val schema = result.right.get
        schema.imports should have size 5
      }

      "measure calculations parsing properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  measure calc(calculation = "a + b / 2")
            |}
          """.stripMargin
        )
        assert(result.isRight)
        val schema = result.right.get
        schema.measures.head shouldBe CalculatedMeasureAST("calc", ref("a") + ref("b") / lit(2))
      }

      "tableRefs parsed properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  table t {
            |    column a(tableRefs = ["t0.a", "t1.b"]) {
            |      tableRef t2.c
            |      tableRef t3.d (joinType = "inner")
            |      tableRef t4.e (joinType = "full")
            |    }
            |  }
            |}
          """.stripMargin
        )
        assert(result.isRight)
        val schema = result.right.get
        val columns = schema.getTable("t").value.columns
        columns.size shouldBe 1
        columns.head.tableRefs shouldBe Set(
          TableRef("t0", "a"),
          TableRef("t1", "b"),
          TableRef("t2", "c"),
          TableRef("t3", "d", JoinType.Inner),
          TableRef("t4", "e", JoinType.Full)
        )
      }

      "metadata parsed properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  measure m {
            |    metadata (s = "String", b = true, n = 3.14, a = ["a", "b", "c"])
            |  }
            |}
          """.stripMargin
        )
        assert(result.isRight)
        val schema = result.right.get
        val m = schema.getMeasure("m").getOrElse(fail("Measure m not found"))
        m.metadata.getOrElse("s", fail("Attribute s not found")) shouldBe StringValue("String")
        m.metadata.getOrElse("b", fail("Attribute b not found")) shouldBe BooleanValue(true)
        m.metadata.getOrElse("n", fail("Attribute n not found")) shouldBe NumericValue(3.14)
        m.metadata.getOrElse("a", fail("Attribute a not found")) shouldBe ArrayValue(
          List(StringValue("a"), StringValue("b"), StringValue("c")))
      }

      "error in determining type of a measure is reported properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  measure m1(aggregate= "avg", filter="a > 2")
            |}
          """.stripMargin
        )
        result shouldBe Left(
          "Params ['filter', 'calculation', 'aggregate'] may not be combined in single measure definition")
      }

      "error in calculated measure definition is reported properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  measure m1(calculation="sales ) + 1")
            |}
          """.stripMargin
        )
        result shouldBe Left(
          "failed to parse numeric expression 'sales ) + 1'. Reason: mismatched input ')' expecting {<EOF>, '.', '+', '-', '/', '*'} at line:1, charPosition:6")
      }

      "error in filtered measure definition is reported properly" in {
        val result = SchemaParser(
          """schema Sales {
            |  measure m1(filter="a >  )")
            |}
          """.stripMargin
        )
        result shouldBe Left(
          "failed to parse filter 'a >  )'. Reason: mismatched input ')' expecting {':', BOOL, NUMBER, TIMESTAMP, DATE, SINGLEQUOTEDSTRINGLIT} at line:1, charPosition:5")
      }

      "unknown parameters are not supported" in {
        val result = SchemaParser("""schema Sales (dataSource = "sales", unknownParameter = "somevalue") {
            |} """.stripMargin)
        assert(result.isLeft)
      }

      "schema without the name shall fail" in {
        val result = SchemaParser("""schema {
                                    |} """.stripMargin)
        assert(result.isLeft)
      }

      "schema can have name consisting of digits and/or characters" in {
        List("0", "a", "000", "abc123", "123abc", "2o2o2o2", "a3a3a3a").foreach { name =>
          val result = SchemaParser(s"""schema $name {
              |} """.stripMargin)
          result shouldBe Right(SchemaAST(name))
        }
      }

      "schema can use various characters in the name" in {
        List("überläßt", "运气", "y-z", "a_b", "$abc", "a-", "-z", "schema", "dimension", "dimensionRef").foreach {
          name =>
            val result = SchemaParser(s"""schema $name {
              |} """.stripMargin)
            result shouldBe Right(SchemaAST(name))
        }
      }

      "schema can not use certain character combinations in the name" in {
        List("1.5", "abc.def", "a.7", "\uD800\uDD99").foreach { name =>
          val result = SchemaParser(s"""schema $name {
              |} """.stripMargin)
          assert(result.isLeft)
        }
      }

      "unknown measure aggregations will fail" in {
        val result = SchemaParser("""schema s {
            |  measure m(aggregate="abc")
            |} """.stripMargin)
        assert(result.isLeft)
      }

      "psl with comments shall succeed" in {
        val result = SchemaParser("""schema someschema { // the comment
            | /*
            | some comment
            | \"
            | ""
            | """.stripMargin + q3 + """
            |  }
            |*/
            |} """.stripMargin)
        assert(result.isRight)
      }
    }

    "correctly parse filter (with different quote contexts)" in {
      def mkSchema(filter: String) = s"""schema X (dataSource = "z") {filter $filter}""".stripMargin

      def getFilter(s: Either[String, SchemaAST]): Either[String, Predicate] =
        s.flatMap(_.defaultFilter.toRight("FILTER IS NOT DEFINED"))

      // \n is supported in triple quotes
      assert(
        getFilter(SchemaParser(mkSchema(s"""$q3 A >
                                  | 'X'$q3""".stripMargin))) == Right(ref("A") > lit("X"))
      )

      assert(getFilter(SchemaParser(mkSchema(s""" 'A > "X" ' """))).isLeft,
             "single quotes are not allowed at top level")

    }
  }

  "Psl with filters" must {
    val bodyParts = {
      List(
        "dimension Time { level year }",
        """filter "GoogleAnalytics.Country in ('US', 'UK') and GATime.y >= 2000" """,
        "measure sessions"
      )
    }

    def mkSchema(parts: List[String]) =
      s"""schema X (dataSource = "z") {${parts.mkString("\n")}}""".stripMargin

    "succeed with one filter line" in {
      bodyParts.permutations.foreach { parts =>
        val schema = mkSchema(parts)
        val r = SchemaParser(mkSchema(parts))
        assert(r.isRight, s"failed to parse schema $schema. Reason: $r")
      }
    }

    "fail with more than one filter line" in {
      (""" filter "A.B > 1" """ :: bodyParts).permutations.foreach { parts =>
        val schema = mkSchema(parts)
        val r = SchemaParser(mkSchema(parts))
        assert(r.isLeft, "succeeded to parse schema with several filters $schema")
        assert(r.left.get.startsWith("More than one filter detected"),
               "Expected parse error related to multiple filters")
      }
    }
  }

  "PSL with empty parameter groups" should {
    "be parsed correctly" in {


      val psl = s"""schema Foodmart() {
                   |  dimension D() {
                   |    metadata()
                   |    hierarchy H() {
                   |      metadata()
                   |      level L() {
                   |        metadata()
                   |        attribute A() {
                   |          metadata()
                   |        }
                   |      }
                   |    }
                   |  }
                   |  measure M(){
                   |     metadata()
                   |  }
                   |  table T(){
                   |    column C(){
                   |      tableRef tbl.col ()
                   |    }
                   |  }
                   |
                   |}
                   |""".stripMargin

      val parsed = SchemaParser(psl)
      parsed shouldBe 'right
      val ast = parsed.right.get

      val dims = ast.dimensions
      val hierarchies = dims.flatMap(_.hierarchies)
      val levels = hierarchies.flatMap(_.levels)
      val attrs = levels.flatMap(_.attributes)
      dims.map(_.name) should contain only("D")
      hierarchies.map(_.name) should contain only("H")
      levels.map(_.name) should contain only("L")
      attrs.map(_.name) should contain only("A")

      ast.measures.map(_.name) should contain only("M")
      ast.tables.map(_.name) should contain only("T")
      ast
        .tables
        .flatMap(_.columns.map(c => c.name -> c.tableRefs.map(r =>  r.tableName -> r.colName))) should contain only
           "C" -> Set("tbl" -> "col")
    }
  }

}
