package pantheon

import java.net.URI

import org.scalatest.{MustMatchers, WordSpec}
import pantheon.planner.ASTBuilders._
import pantheon.util.{Logging, RowSetUtil, Tap}

class PantheonMongoSpec extends WordSpec with MustMatchers {

  implicit val ctx = Logging.newContext

  class Result(statement: Statement) {
    def logicalPlan: String = statement.backendLogicalPlan.toString
    def physicalPlan: String = statement.backendPhysicalPlan.toString

    def value(limit: Int = 100): String = RowSetUtil.toString(statement.execute(), limit)
  }

  val ds =
    MongoSource("foodmart", "127.0.0.1", "foodmart")

  "Pantheon" when {

    def test(schema: String, query: Query)(block: Result => Unit): Unit =
      Pantheon.withConnection(new InMemoryCatalog(Map("_test_" -> schema), List(ds)), "_test_") { connection =>
        val statement = connection.createStatement(query)
        block(new Result(statement))
      }

    "querying mongo" should {
      "single table query" ignore {

        val query = SqlQuery(
          """select c.lname, count(*) as num
            |from customer c
            |group by c.lname
            |order by count(*) desc
            |limit 20""".stripMargin)

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |
            |  table customer {
            |    column customer_id(expression="cast(_MAP['customer_id'] AS integer)")
            |    column fname(expression="cast(_MAP['fname'] AS varchar(30))")
            |    column lname(expression="cast(_MAP['lname'] AS varchar(30))")
            |  }
            |}
            |""".stripMargin

        test(schema, query) { result =>
          result.logicalPlan mustBe
            """LogicalSort(sort0=[$1], dir0=[DESC], fetch=[20])
              |  LogicalAggregate(group=[{0}], num=[COUNT()])
              |    LogicalProject(lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |      MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.physicalPlan mustBe
            """MongoToEnumerableConverter
              |  MongoSort(sort0=[$1], dir0=[DESC], fetch=[20])
              |    MongoAggregate(group=[{0}], num=[COUNT()])
              |      MongoProject(lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |        MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.value(5) mustBe
            """lname=Smith; num=118
              |lname=Wilson; num=51
              |lname=Brown; num=48
              |lname=Williams; num=46
              |lname=Anderson; num=43""".stripMargin
        }
      }

      "query with join" ignore {

        val query = SqlQuery(
          """select c.lname, sum(s.unit_sales) as num
            |from customer c join sales_fact_1998 s on (c.customer_id = s.customer_id)
            |group by c.lname
            |order by sum(s.unit_sales) desc
            |limit 20""".stripMargin)

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |
            |  table customer {
            |    column customer_id(expression="cast(_MAP['customer_id'] AS integer)")
            |    column fname(expression="cast(_MAP['fname'] AS varchar(30))")
            |    column lname(expression="cast(_MAP['lname'] AS varchar(30))")
            |  }
            |
            |  table sales_fact_1998 {
            |     column customer_id(expression="cast(_MAP['customer_id'] AS integer)", tableRef = "customer.customer_id")
            |     column unit_sales (expression="cast(_MAP['unit_sales'] AS integer)")
            |     column store_cost (expression="cast(_MAP['store_cost'] AS double)")
            |  }
            |}
            |""".stripMargin

        test(schema, query) { result =>
          result.logicalPlan mustBe
            """LogicalSort(sort0=[$1], dir0=[DESC], fetch=[20])
              |  LogicalAggregate(group=[{0}], num=[SUM($1)])
              |    LogicalProject(lname=[$2], unit_sales=[$4])
              |      LogicalJoin(condition=[=($0, $3)], joinType=[inner])
              |        LogicalProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], fname=[CAST(ITEM($0, 'fname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"], lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |          MongoTableScan(table=[[foodmart, customer]])
              |        LogicalProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], unit_sales=[CAST(ITEM($0, 'unit_sales')):INTEGER], store_cost=[CAST(ITEM($0, 'store_cost')):DOUBLE])
              |          MongoTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          result.physicalPlan mustBe
            """EnumerableLimit(fetch=[20])
              |  EnumerableSort(sort0=[$1], dir0=[DESC])
              |    EnumerableAggregate(group=[{0}], num=[SUM($1)])
              |      EnumerableCalc(expr#0..3=[{inputs}], lname=[$t1], unit_sales=[$t3])
              |        EnumerableJoin(condition=[=($0, $2)], joinType=[inner])
              |          MongoToEnumerableConverter
              |            MongoProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |              MongoTableScan(table=[[foodmart, customer]])
              |          MongoToEnumerableConverter
              |            MongoProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], unit_sales=[CAST(ITEM($0, 'unit_sales')):INTEGER])
              |              MongoTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          result.value(5) mustBe
            """lname=Smith; num=5490
              |lname=Brown; num=3007
              |lname=Williams; num=2318
              |lname=Campbell; num=2166
              |lname=Clark; num=2140""".stripMargin
        }
      }

      "Olap query single table" ignore {

        val query = AggregateQuery(rows = List("Customer.lastName"),
          orderBy = List(OrderedColumn("Customer.lastName")),
          limit = Some(20)
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |
            |  table customer {
            |    column customer_id(expression="cast(_MAP['customer_id'] AS integer)")
            |    column fname(expression="cast(_MAP['fname'] AS varchar(30))")
            |    column lname(expression="cast(_MAP['lname'] AS varchar(30))")
            |  }
            |}
            |""".stripMargin

        test(schema, query) { result =>
          result.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC], fetch=[20])
              |  LogicalProject(Customer.lastName=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(Customer.lastName=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |        MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.physicalPlan mustBe
            """MongoToEnumerableConverter
              |  MongoSort(sort0=[$0], dir0=[ASC], fetch=[20])
              |    MongoAggregate(group=[{0}])
              |      MongoProject(Customer.lastName=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |        MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.value(5) mustBe
            """Customer.lastName=Abahamdeh
              |Customer.lastName=Abalos
              |Customer.lastName=Abbassi
              |Customer.lastName=Abbate
              |Customer.lastName=Abbey""".stripMargin
        }
      }

      "Olap query multiple tables" ignore {

        val query = AggregateQuery(rows = List("Customer.lastName"),
          measures = List("unitSales"),
          orderBy = List(OrderedColumn("unitSales", SortOrder.Desc)),
          limit = Some(20)
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table customer {
            |    column customer_id(expression="cast(_MAP['customer_id'] AS integer)")
            |    column fname(expression="cast(_MAP['fname'] AS varchar(30))")
            |    column lname(expression="cast(_MAP['lname'] AS varchar(30))")
            |  }
            |
            |  table sales_fact_1998 {
            |     column customer_id(expression="cast(_MAP['customer_id'] AS integer)", tableRef = "customer.customer_id")
            |     column unit_sales (expression="cast(_MAP['unit_sales'] AS integer)")
            |     column store_cost (expression="cast(_MAP['store_cost'] AS double)")
            |  }
            |}
            |""".stripMargin

        test(schema, query) { result =>
          result.logicalPlan mustBe
            """LogicalSort(sort0=[$1], dir0=[DESC-nulls-last], fetch=[20])
              |  LogicalProject(Customer.lastName=[$0], unitSales=[$1])
              |    LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |      LogicalProject(Customer.lastName=[$5], unitSales=[$1])
              |        LogicalJoin(condition=[=($0, $3)], joinType=[left])
              |          LogicalProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], unit_sales=[CAST(ITEM($0, 'unit_sales')):INTEGER], store_cost=[CAST(ITEM($0, 'store_cost')):DOUBLE])
              |            MongoTableScan(table=[[foodmart, sales_fact_1998]])
              |          LogicalProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], fname=[CAST(ITEM($0, 'fname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"], lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |            MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.physicalPlan mustBe
            """EnumerableLimit(fetch=[20])
              |  EnumerableSort(sort0=[$1], dir0=[DESC-nulls-last])
              |    EnumerableAggregate(group=[{0}], unitSales=[SUM($1)])
              |      EnumerableCalc(expr#0..3=[{inputs}], Customer.lastName=[$t3], unitSales=[$t1])
              |        EnumerableJoin(condition=[=($0, $2)], joinType=[left])
              |          MongoToEnumerableConverter
              |            MongoProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], unit_sales=[CAST(ITEM($0, 'unit_sales')):INTEGER])
              |              MongoTableScan(table=[[foodmart, sales_fact_1998]])
              |          MongoToEnumerableConverter
              |            MongoProject(customer_id=[CAST(ITEM($0, 'customer_id')):INTEGER], lname=[CAST(ITEM($0, 'lname')):VARCHAR(30) CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"])
              |              MongoTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          result.value(5) mustBe
            """Customer.lastName=Smith; unitSales=5490
              |Customer.lastName=Brown; unitSales=3007
              |Customer.lastName=Williams; unitSales=2318
              |Customer.lastName=Campbell; unitSales=2166
              |Customer.lastName=Clark; unitSales=2140""".stripMargin
        }
      }

    }
  }
}
