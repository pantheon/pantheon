package pantheon

import org.scalatest.{MustMatchers, WordSpec}
import pantheon.backend.calcite.CalciteConnection
import pantheon.schema.{Column, FoodmartSchemaFixture, PhysicalTableName, SqlExpression}
import org.scalatest.prop.TableDrivenPropertyChecks
import pantheon.schema.DsProviders

import scala.concurrent.duration._
import scala.concurrent.Await
import pantheon.schema.{Table => PTable}
import pantheon.util.Logging

class CalciteConnectionSpec extends WordSpec with MustMatchers with TableDrivenPropertyChecks {

  val fixture = new FoodmartSchemaFixture {}
  val ds = DsProviders.defaultDs
  implicit val ctx = Logging.newContext
  "schema verification logic" should {

    "detect missing tables and columns(negative case))" in {

      val table = Table(
        "table" -> "errors",
        // Physical table cases
        PTable(
          "ptable1",
          Left(PhysicalTableName("sales_fact_19989")),
          ds,
          List(Column("time_id"))
        ) -> List("Table not found"),
        PTable(
          "ptable2",
          Left(PhysicalTableName("sales_fact_1998")),
          ds,
          List(Column("foo"), Column("time_id"), Column("bar"))
        ) -> List("column 'foo' is missing", "column 'bar' is missing"),
        PTable(
          "ptable3",
          Left(PhysicalTableName("sales_fact_1998")),
          ds,
          List(Column("foo", expression = Some("FLOOR(foo_bar)")), Column("bar", expression = Some("FLOOR(whoops)")))
        ) -> List(
          "Some of the following expressions List(FLOOR(foo_bar), FLOOR(whoops)) caused an error: From line 1, column 14 to " +
            "line 1, column 20: Column 'foo_bar' not found in any table"),
        PTable(
          "sql1",
          Right(SqlExpression(s"""select foo from "foodmart"."sales_fact_1998"""")),
          ds
        ) -> List(
          "Error during query execution: From line 1, column 8 to line 1, column 10: Column 'foo' not found in any table"),
        PTable(
          "sql2",
          Right(SqlExpression(s"""select time_id from "foodmart"."sales_fact_1998"""")),
          ds,
          List(Column("foo"), Column("time_id"), Column("bar"))
        ) -> List("column 'foo' is missing", "column 'bar' is missing"),
        PTable(
          "sql3",
          Right(SqlExpression(s"""select time_id from "foodmart"."sales_fact_1998"""")),
          ds,
          List(Column("foo", expression = Some("FLOOR(foo_bar)")), Column("bar", expression = Some("FLOOR(whoops)")))
        ) -> List(
          "Some of the following expressions List(FLOOR(foo_bar), FLOOR(whoops)) caused an error: From line 1, column 14 to line 1, column 20: Column 'foo_bar' not found in any table")
      )

      val conn = CalciteConnection(List(ds))

      table.forEvery { (table, _errs) =>
        Await.result(conn.verifyTable(table), 5.seconds) mustBe _errs
      }
    }

    "no errors in positive cases " in {

      val table = Table(
        "schema",
        PTable("ptable1", Left(PhysicalTableName("sales_fact_1998")), ds),
        PTable(
          "ptable2",
          Left(PhysicalTableName("sales_fact_1998")),
          ds,
          List(
            Column("x", expression = Some("FLOOR(store_sales)")),
            Column("y", expression = Some("FLOOR(store_cost)")),
          ),
          true
        ),
        PTable("ptable3", Right(SqlExpression("""select * from "foodmart"."sales_fact_1998"""")), ds),
        PTable(
          "ptable4",
          Right(SqlExpression("""select * from "foodmart"."sales_fact_1998" limit 1""")),
          ds,
          List(Column("time_id"), Column("store_sales"))
        ),
        PTable(
          "ptable5",
          Right(SqlExpression("""select * from "foodmart"."sales_fact_1998"""")),
          ds,
          List(
            Column("x", expression = Some("FLOOR(time_id)")),
            Column("y", expression = Some("FLOOR(store_sales)"))
          )
        )
      )

      val conn = CalciteConnection(List(ds))
      table.forEvery(table => Await.result(conn.verifyTable(table), 5.seconds) mustBe Nil)
    }
  }
}
