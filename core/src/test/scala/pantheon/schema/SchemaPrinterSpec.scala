package pantheon.schema

import java.sql.Timestamp
import java.sql.Date

import org.scalatest.{Matchers, WordSpec}
import pantheon.planner._
import ASTBuilders._

class SchemaPrinterSpec extends WordSpec with Matchers {

  val q3 = "\"\"\""

  "SchemaPrinter" when {
    "printing schema" should {
      "work correctly" in {
        val schema = SchemaAST(
          "schema1",
          Some("datasource1"),
          false,
          List(
            Dimension(
              "dimension1",
              Some("table1"),
              metadata = Map(
                "name1" -> StringValue("value1"),
                "name2" -> BooleanValue(false),
                "name3" -> NumericValue(12.3),
                "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
              ),
              hierarchies = List(
                Hierarchy(
                  "hierarchy1",
                  Some("table1"),
                  metadata = Map(
                    "name1" -> StringValue("value1"),
                    "name2" -> BooleanValue(false),
                    "name3" -> NumericValue(12.3),
                    "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                  ),
                  levels = List(
                    Level(
                      "level1",
                      Some("table1"),
                      List("column1"),
                      Some("substr(column1, 3)"),
                      Some("varchar"),
                      metadata = Map(
                        "name1" -> StringValue("value1"),
                        "name2" -> BooleanValue(false),
                        "name3" -> NumericValue(12.3),
                        "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                      ),
                      conforms = List("dimension2.attribute1", "dimension3.attribute3"),
                      attributes = List(
                        Attribute(
                          "attribute1",
                          Some("table1"),
                          List("column1"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute2",
                          Some("table1"),
                          List("column2"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute3",
                          Some("table3"),
                          List("column3"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        )
                      )
                    ),
                    Level(
                      "level2",
                      Some("table2"),
                      metadata = Map(
                        "name1" -> StringValue("value1"),
                        "name2" -> BooleanValue(false),
                        "name3" -> NumericValue(12.3),
                        "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                      ),
                      conforms = List("dimension2.attribute1", "dimension3.attribute3"),
                      attributes = List(
                        Attribute(
                          "attribute1",
                          Some("table1"),
                          List("column1"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute2",
                          Some("table1"),
                          List("column2"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute3",
                          Some("table3"),
                          List("column3"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        )
                      )
                    )
                  )
                )
              )
            ),
            Dimension(
              "dimension2",
              Some("table2"),
              metadata = Map(
                "name1" -> StringValue("value1"),
                "name2" -> BooleanValue(false),
                "name3" -> NumericValue(12.3),
                "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
              ),
              hierarchies = List(
                Hierarchy(
                  "",
                  levels = List(
                    Level(
                      "",
                      attributes = List(
                        Attribute("attribute1", Some("table1"), List("column1")),
                        Attribute(
                          "attribute2",
                          Some("table1"),
                          List("column2"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          )
                        ),
                        Attribute("attribute3", Some("table3"), List("column3"))
                      )
                    ),
                    Level(
                      "level2",
                      Some("table2"),
                      metadata = Map(
                        "name1" -> StringValue("value1"),
                        "name2" -> BooleanValue(false),
                        "name3" -> NumericValue(12.3),
                        "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                      ),
                      conforms = List("dimension2.attribute1", "dimension3.attribute3"),
                      attributes = List(
                        Attribute(
                          "attribute1",
                          Some("table1"),
                          List("column1"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute2",
                          Some("table1"),
                          List("column2"),
                          Some("substr(column1, 3)"),
                          Some("varchar"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        ),
                        Attribute(
                          "attribute3",
                          Some("table3"),
                          List("column3"),
                          metadata = Map(
                            "name1" -> StringValue("value1"),
                            "name2" -> BooleanValue(false),
                            "name3" -> NumericValue(12.3),
                            "name4" -> ArrayValue(
                              List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
                          ),
                          conforms = List("dimension2.attribute1", "dimension3.attribute3")
                        )
                      )
                    )
                  )
                )
              )
            ),
            Dimension(
              "dimension3",
              hierarchies = List(
                Hierarchy(
                  "",
                  levels = List(
                    Level(
                      "",
                      attributes = List(
                        Attribute("attribute1", Some("table1"), List("column1")),
                        Attribute("attribute2", Some("table1"), List("column2")),
                        Attribute("attribute3", Some("table3"), List("column3"))
                      )
                    )
                  )
                )
              )
            ),
            Dimension(
              "dimension",
              hierarchies = List(
                Hierarchy(
                  "hierarchy",
                  levels = List(
                    Level(
                      "level",
                      attributes = List(
                        Attribute("012", Some("table1"), List("column1"))
                      )
                    )
                  )
                )
              )
            )
          ), // dimensions
          List(
            AggMeasureAST(
              "measure1",
              MeasureAggregate.Avg,
              List(),
              Some(ref("a") > lit(3)),
              Map(
                "name1" -> StringValue("value1"),
                "name10" -> StringValue("value1\tvalue2\n\t\"test\""),
                "name2" -> BooleanValue(false),
                "name3" -> NumericValue(12.3),
                "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
              ),
              conforms = List("measure2", "measure3")
            ),
            FilteredMeasureAST(
              "measure2",
              "measure1",
              ref("x") === lit(5) & ref("y").isNull & ref("z").isNotNull
            ),
            CalculatedMeasureAST(
              "measure3",
              (ref("x") + lit(2) / lit(3)) * lit(2) - ref("b") * lit(1) + lit(3) * lit(2) / (lit(3) * lit(2)),
              metadata = Map(
                "name1" -> StringValue("value1"),
                "name2" -> BooleanValue(false),
                "name3" -> NumericValue(12.3),
                "name4" -> ArrayValue(List(StringValue("value1"), StringValue("value2"), StringValue("value3")))
              )
            )
          ), // measures
          List(
            TableAST(
              "table1",
              Some(Left(PhysicalTableName("phys_table_1"))),
              Some("datasource1"),
              columns = List(
                Column(
                  "column1",
                  Some("col1 + col2"),
                  dimensionRefs = Set("dimension1.attribute1", "dimension2.attribute3"),
                  measureRefs = Set("measure1", "measure2"),
                  tableRefs = Set(
                    TableRef("table2", "column2", JoinType.Left),
                    TableRef("table3", "column3", JoinType.Left),
                    TableRef("table4", "column4", JoinType.Left)
                  )
                ),
                Column("column2", measureRefs = Set("measure3")),
                Column("column3", dimensionRefs = Set("dimension1.attribute1", "dimension2.attribute3"))
              ),
              wildcardColumns = Nil
            ),
            TableAST(
              "table2",
              columns = List(
                Column(
                  "column1",
                  Some("col1 + col2"),
                  dimensionRefs = Set("dimension1.attribute1", "dimension2.attribute3"),
                  measureRefs = Set("measure1", "measure2"),
                  tableRefs = Set(
                    TableRef("table2", "column2", JoinType.Right),
                    TableRef("table3", "column3", JoinType.Inner),
                    TableRef("table4", "column4", JoinType.Full)
                  )
                ),
                Column("#column2", measureRefs = Set("measure3")),
                Column("column3", dimensionRefs = Set("dimension1.attribute1", "dimension2.attribute3"))
              ),
              wildcardColumns = List((), ())
            )
          ), // tables
          List(
            AliasedName("schema1", Some("s1")),
            AliasedName("schema2", None)
          ), // imports
          List(
            AliasedName("schema3", Some("s3")),
            AliasedName("schema4", None)
          ), // exports
          Some(
            And(
              GreaterThan(UnresolvedField("dimension1.attr1"),
                          literal.Timestamp(Timestamp.valueOf("2017-04-11 11:22:01"))),
              In(UnresolvedField("dimension1.attr2"), Seq(literal.Decimal(1), literal.Decimal(2), literal.Decimal(3)))
            )) // default filter
        )

        SchemaPrinter.print(schema) shouldBe
          s"""schema schema1 (dataSource = "datasource1") {
  import {schema1 => s1}
  import schema2._
  export {schema3 => s3}
  export schema4._

  dimension dimension1 (table = "table1") {
    metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])

    hierarchy hierarchy1 (table = "table1") {
      metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])

      level level1 (table = "table1", column = "column1", expression = "substr(column1, 3)", castType = "varchar") {
        metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
        conforms dimension2.attribute1, dimension3.attribute3

        attribute attribute1 (table = "table1", column = "column1", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute2 (table = "table1", column = "column2", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute3 (table = "table3", column = "column3") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
      }
      level level2 (table = "table2") {
        metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
        conforms dimension2.attribute1, dimension3.attribute3

        attribute attribute1 (table = "table1", column = "column1", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute2 (table = "table1", column = "column2", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute3 (table = "table3", column = "column3") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
      }
    }
  }
  dimension dimension2 (table = "table2") {
    metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])

    hierarchy  {
      level  {
        attribute attribute1 (table = "table1", column = "column1")
        attribute attribute2 (table = "table1", column = "column2") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
        }
        attribute attribute3 (table = "table3", column = "column3")
      }
      level level2 (table = "table2") {
        metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
        conforms dimension2.attribute1, dimension3.attribute3

        attribute attribute1 (table = "table1", column = "column1", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute2 (table = "table1", column = "column2", expression = "substr(column1, 3)", castType = "varchar") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
        attribute attribute3 (table = "table3", column = "column3") {
          metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
          conforms dimension2.attribute1, dimension3.attribute3
        }
      }
    }
  }
  dimension dimension3 {
    hierarchy  {
      level  {
        attribute attribute1 (table = "table1", column = "column1")
        attribute attribute2 (table = "table1", column = "column2")
        attribute attribute3 (table = "table3", column = "column3")
      }
    }
  }
  dimension dimension {
    hierarchy hierarchy {
      level level {
        attribute 012 (table = "table1", column = "column1")
      }
    }
  }

  measure measure1 (aggregate = "Avg", filter = "a > 3") {
    metadata (name3 = 12.3, name10 = ${q3}value1	value2
             |	"test"${q3}, name2 = false, name4 = ["value1", "value2", "value3"], name1 = "value1")
    conforms measure2, measure3
  }
  measure measure2 (ref = "measure1", filter = "x = 5 and y is null and z is not null")
  measure measure3 (calculation = "(x + 2 / 3) * 2 - b * 1 + 3 * 2 / (3 * 2)") {
    metadata (name1 = "value1", name2 = false, name3 = 12.3, name4 = ["value1", "value2", "value3"])
  }

  table table1 (dataSource = "datasource1", physicalTable = "phys_table_1") {
    column column1 (expression = "col1 + col2", tableRefs = ["table2.column2", "table3.column3", "table4.column4"])
    column column2
    column column3
  }
  table table2 {
    columns *
    columns *
    column column1 (expression = "col1 + col2", tableRefs = ["table2.column2 right", "table3.column3 inner", "table4.column4 full"])
    column "#column2"
    column column3
  }

  filter "dimension1.attr1 > timestamp '2017-04-11 11:22:01.0' and dimension1.attr2 in (1, 2, 3)"
}""".stripMargin
      }
    }
    "printing expessions" should {
      "print various types and operators correctly" in {
        val expr =
          ref("a") > lit(Timestamp.valueOf("2017-04-11 11:22:01")) & (
            ref("b").in(lit(1), lit(2), lit(3)) & (
              ref("c").like(lit("pattern1"), lit("pattern2"), lit("pattern3")) |
              ref("d") < lit(Date.valueOf("2017-02-12"))
            ) &
            ref("e").isNull &
            ref("f").isNotNull |
            ref("g").in(lit(10.1), lit(2D), lit(3D)) &
            ref("h") === lit(true)
         )
        SchemaPrinter.printExpression(expr) shouldBe
          "a > timestamp '2017-04-11 11:22:01.0' and ("+
            "b in (1, 2, 3) and (" +
               "c like ('pattern1', 'pattern2', 'pattern3') or d < date '2017-02-12'" +
            ") and e is null and f is not null or g in (10.1, 2.0, 3.0) and h = true" +
          ")"
      }
    }
  }
}
