package pantheon

import java.sql.Date

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{MustMatchers, WordSpec}
import pantheon.planner.{PlannerException, literal}
import pantheon.schema._
import util.RowSetUtil
import pantheon.planner.ASTBuilders._
import pantheon.util._
import cats.syntax.alternative._
import cats.instances.list._
import cats.instances.either._
import org.scalatest.prop.TableDrivenPropertyChecks
import pantheon.util.BoolOps
import DsProviders._
import cats.Later
import PantheonSpec._
import org.scalatest.exceptions.TestFailedException
import pantheon.errors.IncompatibleQueryException
import pantheon.util.Tap
import pantheon.schema.Compatibility.getFields
import pantheon.schema.parser.FilterParser

object PantheonSpec {
  implicit class StatementOps(val statement: Later[Statement]) extends AnyVal {
    def logicalPlan: String =
      statement.value.backendLogicalPlan.toString

    def physicalPlan: String =
      statement.value.backendPhysicalPlan.toString

    def list(limit: Int = 100): List[String] =
      withResource(statement.value.execute()) { rs =>
        RowSetUtil.toStringList(rs, limit)
      }

    def execute(
        limit: Int = 100,
        decimalScale: Int = 3,
        timestampFormat: RowSetUtil.TimestampFormat = RowSetUtil.DateTime
    ): String =
      withResource(statement.value.execute()) { rs =>
        RowSetUtil.toString(rs, limit, decimalScale, timestampFormat)
      }
  }

}

class PantheonSpec extends WordSpec with MustMatchers with StrictLogging with TableDrivenPropertyChecks {

  implicit val ctx = Logging.newContext

  val fixture = new FoodmartSchemaFixture {}

  object TestFunctions {

    def test(psl: String, dsps: Seq[DataSourceProvider], query: PantheonQuery, deps: Map[String, String] = Map.empty)(
        block: (Later[Statement], Later[Statement]) => Unit): Unit =
      testWithSchemaCombined(psl, dsps, query, deps, block)

    def testWithFoodmart(dsps: Seq[DataSourceProvider], query: PantheonQuery, psl: Option[String] = None)(
        block: (Later[Statement], Later[Statement]) => Unit): Unit =
      testWithSchemaCombined(psl.getOrElse(fixture.rootSchema), dsps, query, fixture.allSchemas, block)

    private def testWithSchemaCombined(psl: String,
                                       dsps: Seq[DataSourceProvider],
                                       query: PantheonQuery,
                                       deps: Map[String, String],
                                       block: (Later[Statement], Later[Statement]) => Unit): Unit = {

      def toEntity: PantheonQuery = query match {
        case q: AggregateQuery if q.pivoted => throw new Exception("cannot create Record query from Pivoted query")
        case q: AggregateQuery =>
          if (q.measures.nonEmpty)
            throw new Exception("cannot create Record query from AggregateQuery when measures is non empty")
          else RecordQuery(q.rows, q.filter, q.offset, q.limit, q.orderBy)
        case q: RecordQuery => q
      }

      def logPlans(statement: Statement): Unit = {
        statement match {
          case s: StatementWithPlan => logger.debug(s"Pantheon plan: ${s.plan}")
          case _                    =>
        }
        logger.debug(s"Calcite logical plan: ${statement.backendLogicalPlan}")
        logger.debug(s"Calcite physical plan: ${statement.backendPhysicalPlan}")
      }

      val schemas = deps + ("_test_" -> psl)

      withDs(dsps) { ds =>
        Pantheon.withConnection(new InMemoryCatalog(schemas, List(ds)), "_test_") { connection =>
          block(
            Later(connection.createStatement(query).tap(logPlans)),
            Later(connection.createStatement(toEntity).tap(logPlans))
          )
        }
      }
    }

    def withDs(dsps: Seq[DataSourceProvider])(f: DataSource => Unit): Unit = {
      for {
        dsp <- dsps
        ds <- dsp.dsOpt
      } try {
        f(ds)
      } catch {
        case e: TestFailedException =>
          throw new TestFailedException(
            e.messageFun.andThen(s => Some(s"<DATASOURCE: $dsp>" + s.getOrElse(""))),
            e.cause,
            e.pos,
            e.payload
          )
      }
    }

    def testStatement(psl: String,
                      query: PantheonQuery,
                      deps: Map[String, String] = Map.empty,
                      dataSources: List[DataSource] = List(DsProviders.defaultDs))(block: Statement => Unit): Unit = {
      Pantheon.withConnection(new InMemoryCatalog(Map("_test" -> psl) ++ deps, dataSources), "_test") { connection =>
        block(connection.createStatement(query, Map.empty))
      }
    }
  }

  "Pantheon" when {
    import TestFunctions._

    val q3 = "\"\"\""

    "making error in the query" should {
      "return errors" in {

        val query = AggregateQuery(measures = List("merror"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column time_id
            |    column unit_sales
            |  }
            |}
          """.stripMargin

        intercept[IncompatibleQueryException](
          testStatement(schema, query)(_ => ())
        ).msg mustBe "measures [merror] not found in schema"

      }
    }

    "querying a single table" should {
      "two attributes" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(rows = List("Customer.id", "Customer.firstName", "Customer.lastName"),
                                   orderBy = List(OrderedColumn("Customer.id")))

        def mkSchema(columns: String*) = {
          s"""schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |
            |  table customer {
            |    ${columns.mkString("\n")}
            |  }
            |}
            |""".stripMargin
        }

        forAll(
          Table(
            "Label" -> "Schema",
            "Table with explicit columns" -> mkSchema("column customer_id", "column fname", "column lname"),
            "Table with 'columns *' only" -> mkSchema(" columns *"),
            "Mixed table with regular columns and 'columns *'" -> mkSchema("column fname", "columns *", "column lname")
          ))((_, schema) =>
          test(schema, dataSources, query) { (statement, entityStatement) =>
            statement.logicalPlan mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.firstName=[$1], Customer.lastName=[$2])
              |    LogicalAggregate(group=[{0, 1, 2}])
              |      LogicalProject(Customer.id=[$0], Customer.firstName=[$3], Customer.lastName=[$2])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

            statement.execute(5) mustBe """Customer.id=1; Customer.firstName=Sheri; Customer.lastName=Nowmer
              |Customer.id=2; Customer.firstName=Derrick; Customer.lastName=Whelply
              |Customer.id=3; Customer.firstName=Jeanne; Customer.lastName=Derry
              |Customer.id=4; Customer.firstName=Michael; Customer.lastName=Spence
              |Customer.id=5; Customer.firstName=Maya; Customer.lastName=Gutierrez""".stripMargin

            entityStatement.logicalPlan mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
                |  LogicalProject(Customer.id=[$0], Customer.firstName=[$3], Customer.lastName=[$2])
                |    JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

            entityStatement.execute(5) mustBe """Customer.id=1; Customer.firstName=Sheri; Customer.lastName=Nowmer
              |Customer.id=2; Customer.firstName=Derrick; Customer.lastName=Whelply
              |Customer.id=3; Customer.firstName=Jeanne; Customer.lastName=Derry
              |Customer.id=4; Customer.firstName=Michael; Customer.lastName=Spence
              |Customer.id=5; Customer.firstName=Maya; Customer.lastName=Gutierrez""".stripMargin
        })
      }

      "filtered measures singe table" ignore {
        val q3 = "\"\"\""

        val dataSources = DsProviders.all

        val schema =
          s"""schema Foodmart (dataSource = "foodmart", strict = false) {
             |  dimension Customer(table = "sales_fact_1998") {
             |    attribute id(column = "customer_id")
             |  }
             |
             |  measure unitSales(column = "sales_fact_1998.unit_sales")
             |  measure unitSalesX(measure = "unitSales", filter = "Customer.id > 2000")
             |
             | table sales_fact_1998 {
             |    column unit_sales
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(measures = List("unitSales", "unitSalesX"))

        test(schema, dataSources, query) { (statement, _) =>
          statement.physicalPlan mustBe
            """JdbcToEnumerableConverter
              |  JdbcAggregate(group=[{}], unitSales=[SUM($0)], unitSalesX=[SUM($1)])
              |    JdbcProject(unitSales=[$7], $f2=[CASE(>($2, 2000), $7, null)])
              |      JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(1) mustBe "unitSales=509987.000; unitSalesX=409240.000"
        }
      }

      "filtered and calculated measures multiple tables" ignore {

        val dataSources = DsProviders.without(mysql)
        def mkSchema(customerColumns: String*) =
          s"""schema Foodmart (dataSource = "foodmart") {
             |
             |  dimension Customer(table = "customer") {
             |    attribute id(column = "customer_id")
             |    attribute cars (column = "num_cars_owned")
             |  }
             |
             |  filter  "Customer.id > 1000"
             |  dimension X(table = "sales_fact_1998") {
             |    attribute tid(column = "time_id")
             |  }
             |  //Direct parent child relation
             |  measure unitSales(column = "sales_fact_1998.unit_sales")
             |  measure unitSalesx(measure = "unitSales", filter =  "Customer.cars < 3")
             |
             |  measure numChildren(filter =  "Customer.cars < 3", column = "customer.num_children_at_home")
             |
             |  //Indirect parent child relation
             |  measure storeCost(filter =  "X.tid > 1000", column = "sales_fact_1998.store_cost")
             |  measure storeSales(column = "sales_fact_1998.store_sales")
             |
             |  //storeSales is not used in query directly of via filter
             |  measure calc1 (calculation = "storeSales - storeCost")
             |  //checking division by zero
             |  measure calc2 (calculation = "storeSales + 100000 / ((-1 * storeCost) + storeCost)")
             |
             |  table customer {
             |    ${customerColumns.mkString("\n")}
             |    column num_children_at_home
             |  }
             |  table sales_fact_1998 {
             |     column time_id
             |     column customer_id(tableRef = "customer.customer_id")
             |     column unit_sales
             |     column store_sales
             |     column store_cost
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(
          rows = List("Customer.id"),
          measures = List("calc1", "calc2", "storeCost", "unitSales", "unitSalesx", "numChildren"),
          orderBy = List(OrderedColumn("Customer.id"))
        )

        forAll(
          Table(
            "Label" -> "Schema",
            "Table with explicit columns" -> mkSchema("column customer_id", "column num_cars_owned"),
            "Mixed table with 'columns *'" -> mkSchema("columns *")
          ))((_, schema) =>
          test(schema, dataSources, query) { (statement, _) =>
            statement.logicalPlan mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
                |  LogicalProject(Customer.id=[CASE(IS NOT NULL($0), $0, $2)], calc1=[-($6, $3)], calc2=[+($6, CASE(OR(IS NULL(+(*(-1, $3), $3)), =(+(*(-1, $3), $3), 0)), null, CAST(/(100000, +(*(-1, $3), $3))):DOUBLE NOT NULL))], storeCost=[$3], unitSales=[$4], unitSalesx=[$5], numChildren=[$1])
                |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
                |      LogicalAggregate(group=[{0}], numChildren=[SUM($2)])
                |        LogicalFilter(condition=[<($1, 3)])
                |          LogicalProject(Customer.id=[$0], Customer.cars=[$27], numChildren=[$21])
                |            LogicalFilter(condition=[>($0, 1000)])
                |              JdbcTableScan(table=[[foodmart, customer]])
                |      LogicalAggregate(group=[{0}], storeCost=[SUM($6)], unitSales=[SUM($3)], unitSalesx=[SUM($7)], storeSales=[SUM($4)])
                |        LogicalProject(Customer.id=[$8], X.tid=[$1], Customer.cars=[$35], unitSales=[$7], storeSales=[$5], storeCost=[$6], $f6=[CASE(>($1, 1000), $6, null)], $f7=[CASE(<($35, 3), $7, null)])
                |          LogicalFilter(condition=[>($8, 1000)])
                |            LogicalJoin(condition=[=($2, $8)], joinType=[left])
                |              JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                |              JdbcTableScan(table=[[foodmart, customer]])
                |""".stripMargin

            statement.execute(5, decimalScale = 2) mustBe
              """Customer.id=1001; calc1=6.280; calc2=null; storeCost=3.530; unitSales=3.000; unitSalesx=3.000; numChildren=1
                |Customer.id=1002; calc1=380.180; calc2=null; storeCost=34.210; unitSales=186.000; unitSalesx=186.000; numChildren=2
                |Customer.id=1003; calc1=308.820; calc2=null; storeCost=31.850; unitSales=151.000; unitSalesx=null; numChildren=null
                |Customer.id=1004; calc1=380.370; calc2=null; storeCost=48.030; unitSales=200.000; unitSalesx=null; numChildren=null
                |Customer.id=1005; calc1=348.530; calc2=null; storeCost=12.000; unitSales=165.000; unitSalesx=165.000; numChildren=0""".stripMargin

        })
      }

      "filtered measures optimization" in {

        val dataSources = DsProviders.without(mysql)
        val schema =
          s"""schema Foodmart (dataSource = "foodmart") {
             |
             |  dimension Customer(table = "customer") {
             |    attribute id(column = "customer_id")
             |    attribute cars (column = "num_cars_owned")
             |  }
             |
             |  filter  "Customer.id > 1000"
             |  dimension X(table = "sales_fact_1998") {
             |    attribute tid(column = "time_id")
             |  }
             |  //Direct parent child relation
             |  measure unitSales(column = "sales_fact_1998.unit_sales")
             |  measure unitSalesx(measure = "unitSales", filter =  "Customer.cars < 3")
             |
             |  measure numChildren(filter =  "Customer.cars < 5", column = "customer.num_children_at_home")
             |
             |  //Indirect parent child relation
             |  measure storeCost(filter =  "Customer.cars < 3", column = "sales_fact_1998.store_cost")
             |
             | table customer {
             |    column customer_id
             |    column num_cars_owned
             |    column num_children_at_home
             | }
             | table sales_fact_1998 {
             |    column time_id
             |    column customer_id(tableRef = "customer.customer_id")
             |    column unit_sales
             |    column store_cost
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(
          rows = List("Customer.id"),
          measures = List("storeCost", "unitSalesx", "numChildren"),
          orderBy = List(OrderedColumn("Customer.id"))
        )

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[CASE(IS NOT NULL($0), $0, $2)], storeCost=[$3], unitSalesx=[$4], numChildren=[$1])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], numChildren=[SUM($2)])
              |        LogicalFilter(condition=[<($1, 5)])
              |          LogicalProject(Customer.id=[$0], Customer.cars=[$27], numChildren=[$21])
              |            LogicalFilter(condition=[>($0, 1000)])
              |              JdbcTableScan(table=[[foodmart, customer]])
              |      LogicalAggregate(group=[{0}], storeCost=[SUM($2)], unitSalesx=[SUM($3)])
              |        LogicalFilter(condition=[<($1, 3)])
              |          LogicalProject(Customer.id=[$8], Customer.cars=[$35], storeCost=[$6], unitSales=[$7])
              |            LogicalFilter(condition=[>($8, 1000)])
              |              LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |                JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |                JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(5, decimalScale = 2) mustBe
            """Customer.id=1001; storeCost=3.530; unitSalesx=3.000; numChildren=1
              |Customer.id=1002; storeCost=164.120; unitSalesx=186.000; numChildren=2
              |Customer.id=1003; storeCost=null; unitSalesx=null; numChildren=3
              |Customer.id=1004; storeCost=null; unitSalesx=null; numChildren=1
              |Customer.id=1005; storeCost=148.390; unitSalesx=165.000; numChildren=0""".stripMargin

        }
      }

      "entity only query for dimension" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(rows = List("Customer.id"), orderBy = List(OrderedColumn("Customer.id")))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(columns = ["customer_id", "sales_fact_1998.customer_id"])
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table customer {
            |    column customer_id
            |  }
            |
            |  table sales_fact_1998 {
            |    column customer_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(Customer.id=[$0])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Customer.id=1
              |Customer.id=2
              |Customer.id=3
              |Customer.id=4
              |Customer.id=5""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0])
              |    JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(5) mustBe
            """Customer.id=1
              |Customer.id=2
              |Customer.id=3
              |Customer.id=4
              |Customer.id=5""".stripMargin
        }
      }

      "null shall go last when sorting" in {

        val dataSources = DsProviders.all

        val query1 =
          AggregateQuery(rows = List("Customer.address2"), orderBy = List(OrderedColumn("Customer.address2")))

        val query2 = AggregateQuery(rows = List("Customer.address2"),
                                    orderBy = List(OrderedColumn("Customer.address2", SortOrder.Desc)))

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  dimension Customer(table = "customer") {
            |    attribute address2(column = "address2")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query1) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.address2=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(Customer.address2=[$6])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Customer.address2= Apt. A
              |Customer.address2= Unit A
              |Customer.address2=#1
              |Customer.address2=#10
              |Customer.address2=#101""".stripMargin
        }
        test(schema, dataSources, query2) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])
              |  LogicalProject(Customer.address2=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(Customer.address2=[$6])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Customer.address2=Unit H103
              |Customer.address2=Unit H
              |Customer.address2=Unit G12
              |Customer.address2=Unit G 202
              |Customer.address2=Unit F13""".stripMargin
        }
      }

      "query with many possible permutations of parameters" ignore {

        val dataSources = DsProviders.all

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(columns = ["customer_id", "sales_fact_1998.customer_id"])
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table customer {
            |    column customer_id
            |  }
            |
            |  table sales_fact_1998 {
            |    column customer_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        val queries = {

          def genOrderColumns(names: List[String]): List[OrderedColumn] =
            for {
              sortOrder <- SortOrder.values.toList
              orderColumnName <- names
            } yield OrderedColumn(orderColumnName, sortOrder)

          for {
            (rows, mes) <- List(List("Customer.id") -> Nil,
                                Nil -> List("unitSales"),
                                List("Customer.id") -> List("unitSales"))
            filter <- List(None, Some(ref("Customer.id") >= lit(1)))
            offset <- List(0, 1)
            limit <- List(None, Some(0), Some(1))
            ocs <- genOrderColumns(rows ++ mes)
              .groupBy(_.order)
              .values
              .toList
            // Disabling tests with no order because different databases order reponse differently
            //:+ Nil
          } yield
            AggregateQuery(rows = rows, measures = mes, filter = filter, offset = offset, limit = limit, orderBy = ocs)
              .asInstanceOf[AggregateQuery]
        }

        //checking generation
        queries.size mustBe 72
        queries.size mustBe queries.distinct.size

        def validatePhysicalPlan(q: PantheonQuery, pPlan: String): Seq[String] = {

          def checkAbsent(absent: Seq[String]) =
            absent.flatMap(a => pPlan.contains(a).option(s"$a is found but not expected"))

          def checkPresentInOrder(present: Seq[String]): Seq[String] = {
            present match {
              case Seq()  => Nil
              case Seq(v) => (!pPlan.contains(v)).option(s"$v expected but not found").toSeq
              case s @ (_ +: _) =>
                s.map(p => p -> pPlan.indexOf(p))
                  .sliding(2)
                  .flatMap {
                    case Seq((prev, prevInd), (next, nextInd)) =>
                      (prevInd < 0).option(s"$prev expected but not found") orElse
                        (prevInd >= nextInd).option(s"$prev(index=$prevInd) is encounered before $next(index=$nextInd)")
                  }
                  .toList
            }
          }

          val commonlyPresent = List("JdbcToEnumerableConverter", "JdbcAggregate", "JdbcTableScan")

          val (absent, present) = List(
            q.orderBy.nonEmpty.either("EnumerableSort"),
            ((q.offset > 0 || q.limit.isDefined) && q.orderBy.isEmpty).either("JdbcSort"),
            q.filter.isDefined.either("JdbcFilter")
          ).separate

          //special case, sometimes calcite optimizes harder when limit == 0 or not set
          if (q.limit.forall(_ == 0) && pPlan.trim == "EnumerableValues(tuples=[[]])") Nil
          else
            checkPresentInOrder(commonlyPresent) ++ checkPresentInOrder(present) ++ checkAbsent(absent)
        }

        def validateResponse(q: AggregateQuery, result: StatementOps): Option[String] = {

          val inspectionSize = 3

          val response = result.list(inspectionSize)

          def respEqualsChk(data: List[String]) = {
            (response != data).option(
              s"RESPONSE:\n${response.mkString("\n")}\nWAS NOT EQUAL TO:\n${data.mkString("\n")}(numbers are compared as numbers)\n"
            )
          }

          val orderOpt = {
            Predef.assert(
              q.orderBy.map(_.order).distinct.size <= 1,
              "Both orders are expected to be either Asc or Desc, change this logic if data has changed"
            )
            q.orderBy.headOption.map(_.order)
          }

          val offset = q.offset.ensuring(
            _ <= 1,
            "Expecting offset to be not greater than 1, need to update test data if this is not the case"
          )
          val limit = q.limit
            .getOrElse(inspectionSize)
            .ensuring(
              _ <= inspectionSize,
              s"Expecting limit to be not greater than $inspectionSize, need to update test data if this is not the case")

          (q.rows ::: q.measures) match {
            case List("unitSales") =>
              // Measures-only query is always aggregated(dont care about order)
              respEqualsChk(if (q.limit.contains(0) || offset > 0) Nil else List("unitSales=509987.000"))

            case List("Customer.id") =>
              val dimsDesc = (10281 to 10278).by(-1).map("Customer.id=" + _).drop(offset).take(limit).toList
              val dimsAsc = (1 to 4).map("Customer.id=" + _).drop(offset).take(limit).toList

              // There is only one dimension in the given dataset/schema - Customer.id which is returned in Asc order when queried alone
              // Use ADT based enums to avoid @unchecked
              (orderOpt: @unchecked) match {
                case Some(SortOrder.Asc) | None => respEqualsChk(dimsAsc)
                case Some(SortOrder.Desc)       => respEqualsChk(dimsDesc)
              }

            case List("Customer.id", "unitSales") =>
              val dimsAndMesNoOrd = List(
                "Customer.id=2094; unitSales=250.000",
                "Customer.id=1277; unitSales=215.000",
                "Customer.id=1745; unitSales=250.000",
                "Customer.id=2312; unitSales=179.000"
              ).drop(offset).take(limit)

              val dimsAndMesDesc =
                List(
                  "Customer.id=10281; unitSales=93.000",
                  "Customer.id=10280; unitSales=112.000",
                  "Customer.id=10278; unitSales=80.000",
                  "Customer.id=10277; unitSales=78.000"
                ).drop(offset).take(limit)

              val dimsAndMesAsc = List(
                "Customer.id=3; unitSales=39.000",
                "Customer.id=6; unitSales=21.000",
                "Customer.id=8; unitSales=122.000",
                "Customer.id=9; unitSales=39.000"
              ).drop(offset).take(limit)

              // Use ADT based enums to avoid @unchecked
              (orderOpt: @unchecked) match {
                case Some(SortOrder.Asc)       => respEqualsChk(dimsAndMesAsc)
                case None if q.filter.nonEmpty => respEqualsChk(dimsAndMesAsc)
                case Some(SortOrder.Desc)      => respEqualsChk(dimsAndMesDesc)
                case None                      => respEqualsChk(dimsAndMesNoOrd)
              }

            case List() =>
              throw new IllegalArgumentException("Query having no measures and no dimensions should not be generated")
          }
        }

        queries.foreach(q =>
          test(schema, dataSources, q) { (statement, _) =>
            val pPlan = statement.physicalPlan
            // validatePhysicalPlan(q, pPlan)
            val errors = validateResponse(q, statement)
            val success = errors.isEmpty
            assert(
              success,
              s"\nERRORS:\n${errors.mkString("\n")}\nWHILE TESTING QUERY:'$q'\nHAVING PHYSICAL PLAN:$pPlan"
            )
        })

      }

      "two dimensions with like" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Customer.id", "Customer.lastName"),
          filter = Some(ref("Customer.lastName").like(lit("Cai%"), lit("%ley"))),
          orderBy = List(OrderedColumn("Customer.id"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  dimension Customer(table = "customer") {
            |    attribute lastName(column = "lname")
            |    attribute id(column = "customer_id")
            |
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.lastName=[$1])
              |    LogicalAggregate(group=[{0, 1}])
              |      LogicalProject(Customer.id=[$0], Customer.lastName=[$2])
              |        LogicalFilter(condition=[OR(LIKE($2, 'Cai%'), LIKE($2, '%ley'))])
              |          JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7) mustBe
            """Customer.id=25; Customer.lastName=Conley
              |Customer.id=98; Customer.lastName=Earley
              |Customer.id=199; Customer.lastName=Caijem
              |Customer.id=321; Customer.lastName=Kelley
              |Customer.id=353; Customer.lastName=Bailey
              |Customer.id=479; Customer.lastName=Finley
              |Customer.id=610; Customer.lastName=Bentley""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.lastName=[$2])
              |    LogicalFilter(condition=[OR(LIKE($2, 'Cai%'), LIKE($2, '%ley'))])
              |      JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7) mustBe
            """Customer.id=25; Customer.lastName=Conley
              |Customer.id=98; Customer.lastName=Earley
              |Customer.id=199; Customer.lastName=Caijem
              |Customer.id=321; Customer.lastName=Kelley
              |Customer.id=353; Customer.lastName=Bailey
              |Customer.id=479; Customer.lastName=Finley
              |Customer.id=610; Customer.lastName=Bentley""".stripMargin
        }
      }

      "dimensions with expression" in {

        // ClickHouse does not support dates before 1970 (birth dates)
        val dataSources = DsProviders.without(clickhouse, mysql)

        val query =
          AggregateQuery(
            rows = List("Customer.lastName", "Customer.birthDate", "Customer.birthYear", "Customer.birthMonth"),
            orderBy = List(OrderedColumn("Customer.birthDate"), OrderedColumn("Customer.lastName"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute lastName(column = "lname")
            |    attribute birthDate(column = "birthdate")
            |    attribute birthYear
            |    attribute birthMonth
            |  }
            |
            |  table customer {
            |    column lname
            |    column birthdate
            |    column birthYear(expression = "floor(birthdate to year)")
            |    column birthMonth(expression = "floor(birthdate to month)")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Customer.lastName=[$0], Customer.birthDate=[$1], Customer.birthYear=[$2], Customer.birthMonth=[$3])
              |    LogicalAggregate(group=[{0, 1, 2, 3}])
              |      LogicalProject(Customer.lastName=[$2], Customer.birthDate=[$16], Customer.birthYear=[FLOOR($16, FLAG(YEAR))], Customer.birthMonth=[FLOOR($16, FLAG(MONTH))])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.lastName=Ball; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Kohl; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Salls; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Sparks; Customer.birthDate=1910-01-24; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Bloomberg; Customer.birthDate=1910-02-03; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01
              |Customer.lastName=Alexander; Customer.birthDate=1910-02-10; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01
              |Customer.lastName=Sedwick; Customer.birthDate=1910-02-12; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Customer.lastName=[$2], Customer.birthDate=[$16], Customer.birthYear=[FLOOR($16, FLAG(YEAR))], Customer.birthMonth=[FLOOR($16, FLAG(MONTH))])
              |    JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.lastName=Ball; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Kohl; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Salls; Customer.birthDate=1910-01-06; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Sparks; Customer.birthDate=1910-01-24; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-01-01
              |Customer.lastName=Bloomberg; Customer.birthDate=1910-02-03; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01
              |Customer.lastName=Alexander; Customer.birthDate=1910-02-10; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01
              |Customer.lastName=Sedwick; Customer.birthDate=1910-02-12; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-02-01""".stripMargin
        }
      }

      "be able to floor date to week" in {
        // Hana, DB2 has no support for week truncation
        // Hsqldb has only non-iso week support; truncating to the iso 'IW' in hsql fails
        val dataSources = DsProviders.without(hana, hsqldbFoodmart, db2)

        val query =
          AggregateQuery(
            rows = List(
              "Customer.id",
              "Customer.birthDate",
              "Customer.birthWeek",
            ),
            // filter must be > 1971 for ClickHouse
            filter = Some(ref("Customer.birthDate") >= lit(Date.valueOf("1971-01-01"))),
            orderBy = List(OrderedColumn("Customer.id"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute birthDate(column = "birthdate")
            |    attribute birthWeek
            |  }
            |
            |  table customer {
            |    column customer_id
            |    column birthdate
            |    column birthWeek(expression = "floor(birthdate to week)")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthWeek=[$2])
              |    LogicalAggregate(group=[{0, 1, 2}])
              |      LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthWeek=[$2])
              |        LogicalFilter(condition=[>=($1, 1971-01-01)])
              |          LogicalProject(customer_id=[$0], birthdate=[$16], birthWeek=[FLOOR($16, FLAG(WEEK))])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthWeek=1979-06-18
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthWeek=1971-10-18
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthWeek=1975-10-06
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthWeek=1974-04-15
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthWeek=1978-06-26
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthWeek=1971-07-19
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthWeek=1977-05-16""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthWeek=[$2])
              |    LogicalFilter(condition=[>=($1, 1971-01-01)])
              |      LogicalProject(customer_id=[$0], birthdate=[$16], birthWeek=[FLOOR($16, FLAG(WEEK))])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthWeek=1979-06-18
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthWeek=1971-10-18
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthWeek=1975-10-06
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthWeek=1974-04-15
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthWeek=1978-06-26
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthWeek=1971-07-19
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthWeek=1977-05-16""".stripMargin
        }
      }

      "be able to floor date to month" in {
        val dataSources = DsProviders.without(hana, hsqldbFoodmart, db2)

        val query =
          AggregateQuery(
            rows = List(
              "Customer.id",
              "Customer.birthDate",
              "Customer.birthMonth",
            ),
            // filter must be > 1971 for ClickHouse
            filter = Some(ref("Customer.birthDate") >= lit(Date.valueOf("1971-01-01"))),
            orderBy = List(OrderedColumn("Customer.id"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute birthDate(column = "birthdate")
            |    attribute birthMonth
            |  }
            |
            |  table customer {
            |    column customer_id
            |    column birthdate
            |    column birthMonth(expression = "floor(birthdate to month)")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthMonth=[$2])
              |    LogicalAggregate(group=[{0, 1, 2}])
              |      LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthMonth=[$2])
              |        LogicalFilter(condition=[>=($1, 1971-01-01)])
              |          LogicalProject(customer_id=[$0], birthdate=[$16], birthMonth=[FLOOR($16, FLAG(MONTH))])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthMonth=1979-06-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthMonth=1971-10-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthMonth=1975-10-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthMonth=1974-04-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthMonth=1978-07-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthMonth=1971-07-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthMonth=1977-05-01""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthMonth=[$2])
              |    LogicalFilter(condition=[>=($1, 1971-01-01)])
              |      LogicalProject(customer_id=[$0], birthdate=[$16], birthMonth=[FLOOR($16, FLAG(MONTH))])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthMonth=1979-06-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthMonth=1971-10-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthMonth=1975-10-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthMonth=1974-04-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthMonth=1978-07-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthMonth=1971-07-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthMonth=1977-05-01""".stripMargin
        }
      }

      "be able to floor date to quarter" in {
        val dataSources = DsProviders.without(hana, hsqldbFoodmart, db2, clickhouse, mysql)

        val query =
          AggregateQuery(
            rows = List(
              "Customer.id",
              "Customer.birthDate",
              "Customer.birthQuarter",
            ),
            // filter must be > 1971 for ClickHouse
            filter = Some(ref("Customer.birthDate") >= lit(Date.valueOf("1971-01-01"))),
            orderBy = List(OrderedColumn("Customer.id"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute birthDate(column = "birthdate")
            |    attribute birthQuarter
            |  }
            |
            |  table customer {
            |    column customer_id
            |    column birthdate
            |    column birthQuarter(expression = "floor(birthdate to quarter)")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthQuarter=[$2])
              |    LogicalAggregate(group=[{0, 1, 2}])
              |      LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthQuarter=[$2])
              |        LogicalFilter(condition=[>=($1, 1971-01-01)])
              |          LogicalProject(customer_id=[$0], birthdate=[$16], birthQuarter=[FLOOR($16, FLAG(QUARTER))])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthQuarter=1979-04-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthQuarter=1971-10-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthQuarter=1975-10-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthQuarter=1974-04-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthQuarter=1978-07-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthQuarter=1971-07-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthQuarter=1977-04-01""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthQuarter=[$2])
              |    LogicalFilter(condition=[>=($1, 1971-01-01)])
              |      LogicalProject(customer_id=[$0], birthdate=[$16], birthQuarter=[FLOOR($16, FLAG(QUARTER))])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthQuarter=1979-04-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthQuarter=1971-10-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthQuarter=1975-10-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthQuarter=1974-04-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthQuarter=1978-07-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthQuarter=1971-07-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthQuarter=1977-04-01""".stripMargin
        }
      }

      "be able to floor date to year" in {
        val dataSources = DsProviders.without(hana, hsqldbFoodmart, db2)

        val query =
          AggregateQuery(
            rows = List(
              "Customer.id",
              "Customer.birthDate",
              "Customer.birthYear"
            ),
            // filter must be > 1971 for ClickHouse
            filter = Some(ref("Customer.birthDate") >= lit(Date.valueOf("1971-01-01"))),
            orderBy = List(OrderedColumn("Customer.id"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "customer_id")
            |    attribute birthDate(column = "birthdate")
            |    attribute birthYear
            |  }
            |
            |  table customer {
            |    column customer_id
            |    column birthdate
            |    column birthYear(expression = "floor(birthdate to year)")
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthYear=[$2])
              |    LogicalAggregate(group=[{0, 1, 2}])
              |      LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthYear=[$2])
              |        LogicalFilter(condition=[>=($1, 1971-01-01)])
              |          LogicalProject(customer_id=[$0], birthdate=[$16], birthYear=[FLOOR($16, FLAG(YEAR))])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthYear=1979-01-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthYear=1971-01-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthYear=1975-01-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthYear=1974-01-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthYear=1978-01-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthYear=1971-01-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthYear=1977-01-01""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.birthDate=[$1], Customer.birthYear=[$2])
              |    LogicalFilter(condition=[>=($1, 1971-01-01)])
              |      LogicalProject(customer_id=[$0], birthdate=[$16], birthYear=[FLOOR($16, FLAG(YEAR))])
              |        JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          entityStatement.execute(7, timestampFormat = RowSetUtil.Date) mustBe
            """Customer.id=9; Customer.birthDate=1979-06-23; Customer.birthYear=1979-01-01
               |Customer.id=12; Customer.birthDate=1971-10-18; Customer.birthYear=1971-01-01
               |Customer.id=13; Customer.birthDate=1975-10-12; Customer.birthYear=1975-01-01
               |Customer.id=20; Customer.birthDate=1974-04-16; Customer.birthYear=1974-01-01
               |Customer.id=31; Customer.birthDate=1978-07-02; Customer.birthYear=1978-01-01
               |Customer.id=33; Customer.birthDate=1971-07-23; Customer.birthYear=1971-01-01
               |Customer.id=43; Customer.birthDate=1977-05-16; Customer.birthYear=1977-01-01""".stripMargin
        }
      }

      "dimensions with expression and filter" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Customer.day", "Customer.lastName"),
          measures = List("unitSales"),
          // filter must be > 1971 for ClickHouse
          filter = Some(
            ref("Customer.day") >= lit(Date.valueOf("1971-01-01")) & ref("Customer.day") <= lit(
              Date.valueOf("2013-01-01"))),
          orderBy = List(OrderedColumn("unitSales"), OrderedColumn("Customer.lastName"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount (aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  dimension Customer (table = "customer") {
            |    attribute customer_id (column ="customer_id")
            |    attribute lastName (column = "lname")
            |    attribute birthDate (column = "birthdate")
            |    attribute year (castType = "date", column = "birthYear")
            |    attribute month (castType = "date", column = "birthMonth")
            |    attribute day (castType = "date", column = "birthDay")
            |  }
            |
            |  table sales_fact_1998 {
            |    column customer_id (tableRef = "customer.customer_id")
            |    column time_id
            |    column unit_sales
            |  }
            |  table customer {
            |    column customer_id
            |    column lname
            |    column birthdate
            |    column birthYear(expression = "floor(birthdate to year)")
            |    column birthMonth(expression = "floor(birthdate to month)")
            |    column birthDay(expression = "floor(birthdate to day)")
            |  }
            |}
          """.stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Customer.day=[CAST($0):DATE NOT NULL], Customer.lastName=[$1], unitSales=[$2])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)])
              |      LogicalProject(Customer.day=[$13], Customer.lastName=[$9], unitSales=[$7])
              |        LogicalFilter(condition=[AND(>=($13, 1971-01-01), <=($13, 2013-01-01))])
              |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalProject(customer_id=[$0], lname=[$2], birthdate=[$16], birthYear=[FLOOR($16, FLAG(YEAR))], birthMonth=[FLOOR($16, FLAG(MONTH))], birthDay=[FLOOR($16, FLAG(DAY))])
              |              JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(7) mustBe
            """Customer.day=1974-05-05; Customer.lastName=Banks; unitSales=1.000
              |Customer.day=1978-09-02; Customer.lastName=Pettengill; unitSales=1.000
              |Customer.day=1971-12-17; Customer.lastName=Robey; unitSales=1.000
              |Customer.day=1975-03-02; Customer.lastName=Brandolino; unitSales=2.000
              |Customer.day=1975-12-18; Customer.lastName=Gillis; unitSales=2.000
              |Customer.day=1974-02-06; Customer.lastName=Topliss; unitSales=2.000
              |Customer.day=1973-04-11; Customer.lastName=Willliams; unitSales=2.000""".stripMargin
        }
      }

      "single measure" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(measures = List("unitSales"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table sales_fact_1998 {
            |    column unit_sales
            |  }
            |}
          """.stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalProject(unitSales=[$0])
              |  LogicalAggregate(group=[{}], unitSales=[SUM($0)])
              |    LogicalProject(unitSales=[$7])
              |      JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute() mustBe "unitSales=509987.000"
        }
      }

      "multiple measures" in {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(measures = List("unitSales", "linesCount", "linesCountDistinct", "linesApproxCountDistinct"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |  measure linesCountDistinct(aggregate = "distinctCount", column = "sales_fact_1998.time_id")
            |  measure linesApproxCountDistinct(aggregate = "approxDistinctCount", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column time_id
            |    column unit_sales
            |  }
            |}
          """.stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.execute() mustBe "unitSales=509987.000; linesCount=164558; linesCountDistinct=320; linesApproxCountDistinct=320"
        }
      }

      "single measure, single dimension" in {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(rows = List("Store.id"),
                         measures = List("unitSales"),
                         orderBy = List(OrderedColumn("Store.id")))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Store.id=[$0], unitSales=[$1])
              |    LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |      LogicalProject(Store.id=[$4], unitSales=[$7])
              |        JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Store.id=1; unitSales=23226.000
                |Store.id=2; unitSales=1984.000
                |Store.id=3; unitSales=24069.000
                |Store.id=4; unitSales=23752.000
                |Store.id=5; unitSales=2124.000""".stripMargin
        }
      }

      "single measure, multiple dimensions" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(rows = List("Product.id", "Store.id"),
                                   measures = List("unitSales"),
                                   orderBy = List(OrderedColumn("Product.id"), OrderedColumn("Store.id")))
        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Product.id=[$0], Store.id=[$1], unitSales=[$2])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)])
              |      LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7])
              |        JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Product.id=1; Store.id=1; unitSales=6.000
              |Product.id=1; Store.id=2; unitSales=1.000
              |Product.id=1; Store.id=3; unitSales=5.000
              |Product.id=1; Store.id=4; unitSales=5.000
              |Product.id=1; Store.id=6; unitSales=11.000""".stripMargin
        }
      }

      "multiple measures, multiple dimensionsX" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Product.id", "Store.id"),
          measures = List("unitSales", "linesCount"),
          orderBy = List(OrderedColumn("Product.id"), OrderedColumn("Store.id", SortOrder.Desc))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column time_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC-nulls-last])
              |  LogicalProject(Product.id=[$0], Store.id=[$1], unitSales=[$2], linesCount=[$3])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)], linesCount=[COUNT($3)])
              |      LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7], linesCount=[$1])
              |        JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Product.id=1; Store.id=24; unitSales=3.000; linesCount=1
              |Product.id=1; Store.id=23; unitSales=5.000; linesCount=2
              |Product.id=1; Store.id=22; unitSales=2.000; linesCount=2
              |Product.id=1; Store.id=21; unitSales=13.000; linesCount=4
              |Product.id=1; Store.id=20; unitSales=6.000; linesCount=2""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a queried dimension" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Product.id", "Store.id"),
          measures = List("unitSales", "linesCount"),
          filter = Some(ref("Product.id").in(lit(10), lit(11))),
          orderBy = List(OrderedColumn("Product.id"), OrderedColumn("Store.id"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column time_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Product.id=[$0], Store.id=[$1], unitSales=[$2], linesCount=[$3])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)], linesCount=[COUNT($3)])
              |      LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7], linesCount=[$1])
              |        LogicalFilter(condition=[OR(=($0, 10), =($0, 11))])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Product.id=10; Store.id=1; unitSales=13.000; linesCount=5
              |Product.id=10; Store.id=2; unitSales=2.000; linesCount=2
              |Product.id=10; Store.id=3; unitSales=6.000; linesCount=2
              |Product.id=10; Store.id=4; unitSales=7.000; linesCount=2
              |Product.id=10; Store.id=5; unitSales=1.000; linesCount=1""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a non-queried dimension" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Product.id", "Store.id"),
          measures = List("unitSales", "linesCount"),
          filter = Some(ref("Promotion.id").in(lit(54), lit(58))),
          orderBy = List(OrderedColumn("Product.id"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  dimension Promotion {
            |    attribute id(column = "sales_fact_1998.promotion_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column promotion_id
            |    column time_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Product.id=[$0], Store.id=[$1], unitSales=[$2], linesCount=[$3])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)], linesCount=[COUNT($3)])
              |      LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7], linesCount=[$1])
              |        LogicalFilter(condition=[OR(=($3, 54), =($3, 58))])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(5) mustBe
            """Product.id=3; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=4; Store.id=1; unitSales=5.000; linesCount=2
              |Product.id=5; Store.id=1; unitSales=4.000; linesCount=1
              |Product.id=11; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=13; Store.id=1; unitSales=3.000; linesCount=1""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a multiple queried dimensions" in {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(
            rows = List("Product.id", "Store.id"),
            measures = List("unitSales", "linesCount"),
            filter = Some(
              ref("Product.id").notIn(Seq(10, 11, 12, 13, 14).map(lit(_)): _*) &
                ref("Store.id") >= lit(10) &
                ref("Store.id") <= lit(15)
            ),
            orderBy = List(OrderedColumn("Product.id"), OrderedColumn("Store.id"))
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  dimension Promotion {
            |    attribute id(column = "sales_fact_1998.promotion_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column promotion_id
            |    column time_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Product.id=[$0], Store.id=[$1], unitSales=[$2], linesCount=[$3])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)], linesCount=[COUNT($3)])
              |      LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7], linesCount=[$1])
              |        LogicalFilter(condition=[AND(<>($0, 10), <>($0, 11), <>($0, 12), <>($0, 13), <>($0, 14), >=($4, 10), <=($4, 15))])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Product.id=1; Store.id=10; unitSales=4.000; linesCount=2
              |Product.id=1; Store.id=11; unitSales=6.000; linesCount=2
              |Product.id=1; Store.id=12; unitSales=9.000; linesCount=3
              |Product.id=1; Store.id=13; unitSales=7.000; linesCount=3
              |Product.id=1; Store.id=15; unitSales=5.000; linesCount=2
              |Product.id=2; Store.id=10; unitSales=15.000; linesCount=5""".stripMargin
        }
      }

      "pre and post aggregation filters combined" ignore {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(
            rows = List("Product.id"),
            measures = List("unitSalesFiltered"),
            filter = Some(
              ref("Product.id").notIn(Seq(10, 11, 12, 13, 14).map(lit(_)): _*) &
                ref("Store.id") <= lit(15)
            ),
            orderBy = List(OrderedColumn("Product.id")),
            aggregateFilter = Some(
              ref("unitSalesFiltered") >= lit(7) &
                ref("linesCount") >= lit(3)
            )
          )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  dimension Promotion {
            |    attribute id(column = "sales_fact_1998.promotion_id")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure unitSalesFiltered(measure = "unitSales", filter = "Store.id > 10")
            |  measure linesCount(aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  table sales_fact_1998 {
            |    column store_id
            |    column product_id
            |    column promotion_id
            |    column time_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          println(statement.logicalPlan)

          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Product.id=[$0], unitSalesFiltered=[$1])
              |    LogicalFilter(condition=[AND(>=($1, 7), >=($2, 3))])
              |      LogicalAggregate(group=[{0}], unitSalesFiltered=[SUM($4)], linesCount=[COUNT($3)])
              |        LogicalProject(Product.id=[$0], Store.id=[$4], unitSales=[$7], linesCount=[$1], $f4=[CASE(>($4, 10), $7, null)])
              |          LogicalFilter(condition=[AND(<>($0, 10), <>($0, 11), <>($0, 12), <>($0, 13), <>($0, 14), <=($4, 15))])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Product.id=1; unitSalesFiltered=27.000
              |Product.id=2; unitSalesFiltered=54.000
              |Product.id=3; unitSalesFiltered=76.000
              |Product.id=4; unitSalesFiltered=43.000
              |Product.id=5; unitSalesFiltered=60.000
              |Product.id=6; unitSalesFiltered=72.000""".stripMargin
        }
      }

      "single measure, single star dimension" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Customer.lastName"),
          measures = List("unitSales"),
          orderBy = List(OrderedColumn("Customer.lastName"), OrderedColumn("unitSales"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute lastName(column = "lname")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table customer {
            |    column customer_id
            |    column lname
            |  }
            |
            |  table sales_fact_1998 {
            |    column customer_id (tableRef = "customer.customer_id")
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Customer.lastName=[$0], unitSales=[$1])
              |    LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |      LogicalProject(Customer.lastName=[$10], unitSales=[$7])
              |        LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Customer.lastName=Abahamdeh; unitSales=17.000
              |Customer.lastName=Abalos; unitSales=48.000
              |Customer.lastName=Abbassi; unitSales=3.000
              |Customer.lastName=Abbate; unitSales=83.000
              |Customer.lastName=Abbey; unitSales=14.000
              |Customer.lastName=Abbott; unitSales=79.000""".stripMargin
        }
      }

      "single measure, single datetime dimension" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Date.date"),
          measures = List("unitSales"),
          orderBy = List(OrderedColumn("Date.date"), OrderedColumn("unitSales"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Date(table = "time_by_day") {
            |    attribute date(column = "the_date")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table time_by_day {
            |    column time_id
            |    column the_date
            |  }
            |
            |  table sales_fact_1998 {
            |    column time_id (tableRef = "time_by_day.time_id")
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Date.date=[$0], unitSales=[$1])
              |    LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |      LogicalProject(Date.date=[$9], unitSales=[$7])
              |        LogicalJoin(condition=[=($1, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Date.date=1998-01-01 00:00:00; unitSales=633.000
              |Date.date=1998-01-02 00:00:00; unitSales=1797.000
              |Date.date=1998-01-03 00:00:00; unitSales=1460.000
              |Date.date=1998-01-04 00:00:00; unitSales=1792.000
              |Date.date=1998-01-05 00:00:00; unitSales=2439.000
              |Date.date=1998-01-06 00:00:00; unitSales=1158.000""".stripMargin
        }
      }
    }

    "querying across multiple tables" should {
      "single measures, no dimensions" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(measures = List("unitSales", "warehouseSales"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Product {
            |    attribute id(columns = ["sales_fact_1998.product_id", "inventory_fact_1998.product_id"])
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
            |
            |  table sales_fact_1998 {
            |    column product_id
            |    column unit_sales
            |  }
            |
            |  table inventory_fact_1998 {
            |    column product_id
            |    column warehouse_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalProject(unitSales=[$0], warehouseSales=[$1])
              |  LogicalJoin(condition=[true], joinType=[full])
              |    LogicalAggregate(group=[{}], unitSales=[SUM($0)])
              |      LogicalProject(unitSales=[$7])
              |        JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |    LogicalAggregate(group=[{}], warehouseSales=[SUM($0)])
              |      LogicalProject(warehouseSales=[$6])
              |        JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |""".stripMargin

          statement.execute() mustBe
            """unitSales=509987.000; warehouseSales=348484.720""".stripMargin
        }
      }

      "single measures, degenerate dimension" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Product.id"),
          measures = List("unitSales", "warehouseSales"),
          orderBy = List(OrderedColumn("Product.id"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Product {
            |    attribute id(columns = ["sales_fact_1998.product_id", "inventory_fact_1998.product_id"])
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
            |
            |  table sales_fact_1998 {
            |    column product_id
            |    column unit_sales
            |  }
            |
            |  table inventory_fact_1998 {
            |    column product_id
            |    column warehouse_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Product.id=[CASE(IS NOT NULL($0), $0, $2)], unitSales=[$1], warehouseSales=[$3])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |        LogicalProject(Product.id=[$0], unitSales=[$7])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |      LogicalAggregate(group=[{0}], warehouseSales=[SUM($1)])
              |        LogicalProject(Product.id=[$0], warehouseSales=[$6])
              |          JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Product.id=1; unitSales=148.000; warehouseSales=37.392
              |Product.id=2; unitSales=275.000; warehouseSales=84.212
              |Product.id=3; unitSales=266.000; warehouseSales=130.684
              |Product.id=4; unitSales=278.000; warehouseSales=186.004
              |Product.id=5; unitSales=279.000; warehouseSales=514.475
              |Product.id=6; unitSales=318.000; warehouseSales=93.909""".stripMargin
        }
      }

      "single dimension, measures from different fact tables" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Product.name"),
          measures = List("unitSales", "warehouseSales"),
          orderBy = List(OrderedColumn("Product.name"), OrderedColumn("unitSales"))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  dimension Product(table = "product") {
            |    attribute id(columns = ["product_id", "sales_fact_1998.product_id", "inventory_fact_1998.product_id"])
            |    attribute name(column = "product_name")
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
            |
            |  table sales_fact_1998 {
            |    column product_id (tableRef = "product.product_id")
            |    column unit_sales
            |  }
            |
            |  table inventory_fact_1998 {
            |    column product_id (tableRef = "product.product_id")
            |    column warehouse_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Product.name=[CASE(IS NOT NULL($0), $0, $2)], unitSales=[$1], warehouseSales=[$3])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
              |        LogicalProject(Product.name=[$11], unitSales=[$7])
              |          LogicalJoin(condition=[=($0, $9)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, product]])
              |      LogicalAggregate(group=[{0}], warehouseSales=[SUM($1)])
              |        LogicalProject(Product.name=[$13], warehouseSales=[$6])
              |          LogicalJoin(condition=[=($0, $11)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, product]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Product.name=ADJ Rosy Sunglasses; unitSales=419.000; warehouseSales=421.976
              |Product.name=Akron City Map; unitSales=333.000; warehouseSales=274.015
              |Product.name=Akron Eyeglass Screwdriver; unitSales=375.000; warehouseSales=305.976
              |Product.name=American Beef Bologna; unitSales=342.000; warehouseSales=41.980
              |Product.name=American Chicken Hot Dogs; unitSales=272.000; warehouseSales=175.165
              |Product.name=American Cole Slaw; unitSales=367.000; warehouseSales=99.929""".stripMargin
        }
      }

      "filtering on non-queried dimension attribute" in {
        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("sales.Store.region.city"),
          measures = List("sales.sales"),
          filter = Some(ref("sales.Store.region.country") === lit("USA")),
          orderBy = List(OrderedColumn("sales.Store.region.city"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(sales.Store.region.city=[$0], sales.sales=[$1])
              |    LogicalAggregate(group=[{0}], sales.sales=[SUM($1)])
              |      LogicalProject(sales.Store.region.city=[$33], sales.sales=[$5])
              |        LogicalFilter(condition=[=($37, 'USA')])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """sales.Store.region.city=Bellingham; sales.sales=4164.810
              |sales.Store.region.city=Beverly Hills; sales.sales=47843.920
              |sales.Store.region.city=Bremerton; sales.sales=51135.190
              |sales.Store.region.city=Los Angeles; sales.sales=50819.150
              |sales.Store.region.city=Portland; sales.sales=53633.260
              |sales.Store.region.city=Salem; sales.sales=74965.240""".stripMargin
        }
      }

      "filter with OR/grouping" in {
        val dataSources = DsProviders.all

        def mkQuery(filter: String) = AggregateQuery(
          rows = List("inv.Date.Month"),
          filter = Some(FilterParser(filter).right.get),
          orderBy = List(OrderedColumn("inv.Date.Month"))
        )

        testWithFoodmart(
          dataSources,
          mkQuery("inv.Date.Month > 'A' and inv.Date.Month < 'D' or inv.Date.Month <= 'Z' and inv.Date.Month > 'K'")) {
          (statement, _) =>
            statement.logicalPlan mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(inv.Date.Month=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(inv.Date.Month=[$3])
              |        LogicalFilter(condition=[OR(AND(>($3, 'A'), <($3, 'D')), AND(<=($3, 'Z'), >($3, 'K')))])
              |          JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

            statement.execute(12) mustBe
              """inv.Date.Month=April
              |inv.Date.Month=August
              |inv.Date.Month=March
              |inv.Date.Month=May
              |inv.Date.Month=November
              |inv.Date.Month=October
              |inv.Date.Month=September""".stripMargin
        }

        testWithFoodmart(
          dataSources,
          mkQuery("inv.Date.Month > 'A' and (inv.Date.Month < 'D' or inv.Date.Month <= 'Z') and inv.Date.Month > 'K'")) {
          (statement, _) =>
            statement.logicalPlan mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(inv.Date.Month=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(inv.Date.Month=[$3])
              |        LogicalFilter(condition=[AND(OR(<($3, 'D'), <=($3, 'Z')), >($3, 'K'))])
              |          JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

            statement.execute(12) mustBe
              """inv.Date.Month=March
              |inv.Date.Month=May
              |inv.Date.Month=November
              |inv.Date.Month=October
              |inv.Date.Month=September""".stripMargin
        }
      }

      "Combination of limit with filtering and sorting" in {
        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("sales.Store.region.country", "sales.Store.region.city"),
          measures = List("sales.sales"),
          filter = Some(ref("sales.Store.region.country") === lit("USA")),
          orderBy = List(OrderedColumn("sales.sales", SortOrder.Desc)),
          limit = Some(5)
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$2], dir0=[DESC-nulls-last], fetch=[5])
              |  LogicalProject(sales.Store.region.country=[$0], sales.Store.region.city=[$1], sales.sales=[$2])
              |    LogicalAggregate(group=[{0, 1}], sales.sales=[SUM($2)])
              |      LogicalProject(sales.Store.region.country=[$37], sales.Store.region.city=[$33], sales.sales=[$5])
              |        LogicalFilter(condition=[=($37, 'USA')])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(5) mustBe
            """sales.Store.region.country=USA; sales.Store.region.city=Tacoma; sales.sales=75219.130
              |sales.Store.region.country=USA; sales.Store.region.city=Salem; sales.sales=74965.240
              |sales.Store.region.country=USA; sales.Store.region.city=Seattle; sales.sales=56579.460
              |sales.Store.region.country=USA; sales.Store.region.city=Spokane; sales.sales=54984.940
              |sales.Store.region.country=USA; sales.Store.region.city=Portland; sales.sales=53633.260""".stripMargin
        }
      }
    }

    "querying with exported schemas" should {
      "dimension from exported schema" in {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(rows = List("sales.Store.region.city"),
                         orderBy = List(OrderedColumn("sales.Store.region.city")))

        testWithFoodmart(dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(sales.Store.region.city=[$0])
              |    LogicalAggregate(group=[{0}])
              |      LogicalProject(sales.Store.region.city=[$1])
              |        JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """sales.Store.region.city=Acapulco
              |sales.Store.region.city=Albany
              |sales.Store.region.city=Altadena
              |sales.Store.region.city=Anacortes
              |sales.Store.region.city=Arcadia
              |sales.Store.region.city=Ballard""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(sales.Store.region.city=[$1])
              |    JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          entityStatement.execute(6) mustBe
            """sales.Store.region.city=Acapulco
              |sales.Store.region.city=Albany
              |sales.Store.region.city=Altadena
              |sales.Store.region.city=Anacortes
              |sales.Store.region.city=Arcadia
              |sales.Store.region.city=Ballard""".stripMargin
        }
      }

      "measure and dimension from exported schema" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("sales.Store.region.city"),
          measures = List("sales.sales"),
          orderBy = List(OrderedColumn("sales.Store.region.city"), OrderedColumn("sales.sales"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(sales.Store.region.city=[$0], sales.sales=[$1])
              |    LogicalAggregate(group=[{0}], sales.sales=[SUM($1)])
              |      LogicalProject(sales.Store.region.city=[$33], sales.sales=[$5])
              |        LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, store]])
              |            JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """sales.Store.region.city=Acapulco; sales.sales=49090.030
              |sales.Store.region.city=Bellingham; sales.sales=4164.810
              |sales.Store.region.city=Beverly Hills; sales.sales=47843.920
              |sales.Store.region.city=Bremerton; sales.sales=51135.190
              |sales.Store.region.city=Camacho; sales.sales=50047.510
              |sales.Store.region.city=Guadalajara; sales.sales=4328.870""".stripMargin
        }
      }

      "Encoding test" in {

        val dataSources = DsProviders.all

        val query =
          AggregateQuery(rows = List("sales.Store.region.city"),
                         filter = Some(ref("sales.Store.region.city") === lit("Beats X für 49€")))

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalProject(sales.Store.region.city=[$0])
              |  LogicalAggregate(group=[{0}])
              |    LogicalProject(sales.Store.region.city=[$1])
              |      LogicalFilter(condition=[=($1, u&'Beats X f\00fcr 49\20ac')])
              |        JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe ""
        }
      }

      "conforming dimension and measure from exported schema" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Store.region.city"),
          measures = List("sales.sales"),
          orderBy = List(OrderedColumn("Store.region.city"), OrderedColumn("sales.sales"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[$0], sales.sales=[$1])
              |    LogicalAggregate(group=[{0}], sales.sales=[SUM($1)])
              |      LogicalProject(Store.region.city=[$33], sales.sales=[$5])
              |        LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, store]])
              |            JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=Acapulco; sales.sales=49090.030
              |Store.region.city=Bellingham; sales.sales=4164.810
              |Store.region.city=Beverly Hills; sales.sales=47843.920
              |Store.region.city=Bremerton; sales.sales=51135.190
              |Store.region.city=Camacho; sales.sales=50047.510
              |Store.region.city=Guadalajara; sales.sales=4328.870""".stripMargin
        }
      }

      "conforming dimension, dimension and measure from exported schema" in {
        val dataSources = DsProviders.all
        val query = AggregateQuery(
          rows = List("Store.region.city", "sales.Customer.customer.lastName"),
          measures = List("sales.sales"),
          orderBy = List(OrderedColumn("sales.Customer.customer.lastName"), OrderedColumn("sales.sales"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[$0], sales.Customer.customer.lastName=[$1], sales.sales=[$2])
              |    LogicalAggregate(group=[{0, 1}], sales.sales=[SUM($2)])
              |      LogicalProject(Store.region.city=[$62], sales.Customer.customer.lastName=[$10], sales.sales=[$5])
              |        LogicalJoin(condition=[=($4, $37)], joinType=[left])
              |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |          LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, store]])
              |            JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=San Diego; sales.Customer.customer.lastName=Abahamdeh; sales.sales=39.900
              |Store.region.city=San Diego; sales.Customer.customer.lastName=Abalos; sales.sales=72.110
              |Store.region.city=Victoria; sales.Customer.customer.lastName=Abbassi; sales.sales=8.220
              |Store.region.city=Salem; sales.Customer.customer.lastName=Abbate; sales.sales=147.360
              |Store.region.city=Seattle; sales.Customer.customer.lastName=Abbey; sales.sales=30.330
              |Store.region.city=Los Angeles; sales.Customer.customer.lastName=Abbott; sales.sales=15.260""".stripMargin
        }
      }

      "conforming dimension, measures from different exported schemas" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Store.region.city"),
          measures = List("sales.sales", "inv.warehouseSales"),
          orderBy = List(OrderedColumn("Store.region.city"), OrderedColumn("sales.sales"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[CASE(IS NOT NULL($0), $0, $2)], sales.sales=[$3], inv.warehouseSales=[$1])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], inv.warehouseSales=[SUM($1)])
              |        LogicalProject(Store.region.city=[$35], inv.warehouseSales=[$6])
              |          LogicalJoin(condition=[=($3, $10)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0}], sales.sales=[SUM($1)])
              |        LogicalProject(Store.region.city=[$33], sales.sales=[$5])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=Acapulco; sales.sales=49090.030; inv.warehouseSales=23817.119
              |Store.region.city=Bellingham; sales.sales=4164.810; inv.warehouseSales=2026.280
              |Store.region.city=Beverly Hills; sales.sales=47843.920; inv.warehouseSales=9434.614
              |Store.region.city=Bremerton; sales.sales=51135.190; inv.warehouseSales=7260.913
              |Store.region.city=Camacho; sales.sales=50047.510; inv.warehouseSales=23140.649
              |Store.region.city=Guadalajara; sales.sales=4328.870; inv.warehouseSales=2065.922""".stripMargin
        }
      }

      "a couple of conforming dimension, measures from different exported schemas" in {

        val dataSources = DsProviders.without(mysql)

        val query =
          AggregateQuery(
            rows = List("Store.region.city", "Date.Month"),
            measures = List("sales.sales", "inv.warehouseSales"),
            orderBy = List(OrderedColumn("Store.region.city"), OrderedColumn("sales.sales"))
          )
        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$2], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[CASE(IS NOT NULL($0), $0, $3)], Date.Month=[CASE(IS NOT NULL($1), $1, $4)], sales.sales=[$5], inv.warehouseSales=[$2])
              |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
              |      LogicalAggregate(group=[{0, 1}], inv.warehouseSales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$45], Date.Month=[$13], inv.warehouseSales=[$6])
              |          LogicalJoin(condition=[=($3, $20)], joinType=[left])
              |            LogicalJoin(condition=[=($1, $10)], joinType=[inner])
              |              JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |              JdbcTableScan(table=[[foodmart, time_by_day]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0, 1}], sales.sales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$43], Date.Month=[$11], sales.sales=[$5])
              |          LogicalJoin(condition=[=($4, $18)], joinType=[left])
              |            LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |              JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |              JdbcTableScan(table=[[foodmart, time_by_day]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=Acapulco; Date.Month=June; sales.sales=3384.400; inv.warehouseSales=2240.801
              |Store.region.city=Acapulco; Date.Month=August; sales.sales=3686.990; inv.warehouseSales=2948.813
              |Store.region.city=Acapulco; Date.Month=February; sales.sales=4212.250; inv.warehouseSales=3010.701
              |Store.region.city=Acapulco; Date.Month=October; sales.sales=4301.570; inv.warehouseSales=773.442
              |Store.region.city=Acapulco; Date.Month=May; sales.sales=4312.830; inv.warehouseSales=2604.161
              |Store.region.city=Acapulco; Date.Month=July; sales.sales=4396.120; inv.warehouseSales=2047.617""".stripMargin

        }
      }

      "a couple of conforming dimension, measures from different exported schemas with filtering" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales.sales", "inv.warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998))),
          orderBy = List(OrderedColumn("Store.region.city"), OrderedColumn("sales.sales"))
        )
        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$2], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[CASE(IS NOT NULL($0), $0, $3)], Date.Month=[CASE(IS NOT NULL($1), $1, $4)], sales.sales=[$5], inv.warehouseSales=[$2])
              |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
              |      LogicalAggregate(group=[{0, 1}], inv.warehouseSales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$45], Date.Month=[$13], inv.warehouseSales=[$6])
              |          LogicalFilter(condition=[OR(=($14, 1997), =($14, 1998))])
              |            LogicalJoin(condition=[=($3, $20)], joinType=[left])
              |              LogicalJoin(condition=[=($1, $10)], joinType=[inner])
              |                JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |                JdbcTableScan(table=[[foodmart, time_by_day]])
              |              LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |                JdbcTableScan(table=[[foodmart, store]])
              |                JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0, 1}], sales.sales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$43], Date.Month=[$11], sales.sales=[$5])
              |          LogicalFilter(condition=[OR(=($12, 1997), =($12, 1998))])
              |            LogicalJoin(condition=[=($4, $18)], joinType=[left])
              |              LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |                JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |                JdbcTableScan(table=[[foodmart, time_by_day]])
              |              LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |                JdbcTableScan(table=[[foodmart, store]])
              |                JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=Acapulco; Date.Month=June; sales.sales=3384.400; inv.warehouseSales=2240.801
              |Store.region.city=Acapulco; Date.Month=August; sales.sales=3686.990; inv.warehouseSales=2948.813
              |Store.region.city=Acapulco; Date.Month=February; sales.sales=4212.250; inv.warehouseSales=3010.701
              |Store.region.city=Acapulco; Date.Month=October; sales.sales=4301.570; inv.warehouseSales=773.442
              |Store.region.city=Acapulco; Date.Month=May; sales.sales=4312.830; inv.warehouseSales=2604.161
              |Store.region.city=Acapulco; Date.Month=July; sales.sales=4396.120; inv.warehouseSales=2047.617""".stripMargin
        }
      }

      "conforming dimension and two measures" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Store.store.type"),
          measures = List("warehouseSales", "sales"),
          orderBy = List(OrderedColumn("Store.store.type"))
        )

        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], dir0=[ASC])
              |  LogicalProject(Store.store.type=[CASE(IS NOT NULL($0), $0, $2)], warehouseSales=[$1], sales=[$3])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], warehouseSales=[SUM($1)])
              |        LogicalProject(Store.store.type=[$11], warehouseSales=[$6])
              |          LogicalJoin(condition=[=($3, $10)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, store]])
              |      LogicalAggregate(group=[{0}], sales=[SUM($1)])
              |        LogicalProject(Store.store.type=[$9], sales=[$5])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, store]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.store.type=Deluxe Supermarket; warehouseSales=115840.940; sales=458607.950
              |Store.store.type=Gourmet Supermarket; warehouseSales=32575.263; sales=97891.430
              |Store.store.type=Mid-Size Grocery; warehouseSales=39623.333; sales=85071.940
              |Store.store.type=Small Grocery; warehouseSales=8391.591; sales=17571.650
              |Store.store.type=Supermarket; warehouseSales=152053.593; sales=420004.500""".stripMargin
        }
      }

      "a couple of conforming dimension, conforming measures from different schemas with filtering" in {

        val dataSources = DsProviders.without(mysql)

        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998))),
          orderBy = List(OrderedColumn("Store.region.city"), OrderedColumn("sales"))
        )
        testWithFoodmart(dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$2], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region.city=[CASE(IS NOT NULL($0), $0, $3)], Date.Month=[CASE(IS NOT NULL($1), $1, $4)], sales=[$5], warehouseSales=[$2])
              |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
              |      LogicalAggregate(group=[{0, 1}], warehouseSales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$45], Date.Month=[$13], warehouseSales=[$6])
              |          LogicalFilter(condition=[OR(=($14, 1997), =($14, 1998))])
              |            LogicalJoin(condition=[=($3, $20)], joinType=[left])
              |              LogicalJoin(condition=[=($1, $10)], joinType=[inner])
              |                JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |                JdbcTableScan(table=[[foodmart, time_by_day]])
              |              LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |                JdbcTableScan(table=[[foodmart, store]])
              |                JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0, 1}], sales=[SUM($2)])
              |        LogicalProject(Store.region.city=[$43], Date.Month=[$11], sales=[$5])
              |          LogicalFilter(condition=[OR(=($12, 1997), =($12, 1998))])
              |            LogicalJoin(condition=[=($4, $18)], joinType=[left])
              |              LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |                JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |                JdbcTableScan(table=[[foodmart, time_by_day]])
              |              LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |                JdbcTableScan(table=[[foodmart, store]])
              |                JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region.city=Acapulco; Date.Month=June; sales=3384.400; warehouseSales=2240.801
              |Store.region.city=Acapulco; Date.Month=August; sales=3686.990; warehouseSales=2948.813
              |Store.region.city=Acapulco; Date.Month=February; sales=4212.250; warehouseSales=3010.701
              |Store.region.city=Acapulco; Date.Month=October; sales=4301.570; warehouseSales=773.442
              |Store.region.city=Acapulco; Date.Month=May; sales=4312.830; warehouseSales=2604.161
              |Store.region.city=Acapulco; Date.Month=July; sales=4396.120; warehouseSales=2047.617""".stripMargin
        }
      }

      "incompatible dimensions from multiple exported schemas" in {
        val dataSources = Seq(DsProviders.defaultProvider)
        val query = AggregateQuery(rows = List("sales.Date.Month", "inv.Date.Month"))

        testWithFoodmart(dataSources, query) { (statement, _) =>
          assertThrows[PlannerException] {
            statement.logicalPlan
          }
        }
      }

      "incompatible dimension and measure from different exported schemas" in {
        val dataSources = Seq(DsProviders.defaultProvider)
        val query = AggregateQuery(rows = List("sales.Date.Month"), measures = List("inv.warehouseSales"))

        testWithFoodmart(dataSources, query) { (statement, _) =>
          assertThrows[PlannerException] {
            statement.logicalPlan
          }
        }
      }

    }

    "querying a view on table" should {
      "dimensions only" in {
        val dataSources = DsProviders.all
        val query = AggregateQuery(
          rows = List("Customer.id", "Customer.year", "Customer.day", "Customer.month"),
          orderBy = List(OrderedColumn("Customer.id"), OrderedColumn("Customer.month"), OrderedColumn("Customer.day"))
        )

        val schema =
          s"""schema Foodmart (dataSource = "foodmart"){
             | // \\n is supported in tripplequotes
             | table viewTable(sql = $q3
             |   select * from "foodmart"."sales_fact_1998" sf8
             |   join "foodmart"."time_by_day" td
             |   on (sf8.time_id = td.time_id)
             | $q3) {
             |    column customer_id
             |    column the_year
             |    column the_month
             |    column day_of_month
             |    column day_of_week(expression = $q3 extract(day from "the_date")$q3)
             |  }
             |
             |  dimension Customer(table = "viewTable") {
             |    attribute id(column = "customer_id")
             |    attribute year(column = "the_year")
             |    attribute month(column = "the_month")
             |    attribute day(column = "day_of_month")
             |  }
             |}
             |""".stripMargin

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$3], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.year=[$1], Customer.day=[$2], Customer.month=[$3])
              |    LogicalAggregate(group=[{0, 1, 2, 3}])
              |      LogicalProject(Customer.id=[$2], Customer.year=[$12], Customer.day=[$13], Customer.month=[$11])
              |        LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$3], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])
              |  LogicalProject(Customer.id=[$2], Customer.year=[$12], Customer.day=[$13], Customer.month=[$11])
              |    LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |      JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |      JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=29; Customer.month=March
              |Customer.id=6; Customer.year=1998; Customer.day=16; Customer.month=January
              |Customer.id=6; Customer.year=1998; Customer.day=25; Customer.month=June
              |Customer.id=8; Customer.year=1998; Customer.day=15; Customer.month=April
              |Customer.id=8; Customer.year=1998; Customer.day=17; Customer.month=July""".stripMargin

          entityStatement.execute(6) mustBe
            """Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April
              |Customer.id=3; Customer.year=1998; Customer.day=17; Customer.month=April""".stripMargin
        }

      }

      "dimensions and measures" in {
        val dataSources = DsProviders.all
        val query = AggregateQuery(
          rows = List("Customer.id", "Customer.month", "Customer.day"),
          measures = List("unitSales"),
          orderBy = List(OrderedColumn("Customer.id"), OrderedColumn("unitSales"))
        )
        val schema =
          s"""schema Foodmart(dataSource = "foodmart")  {
             |
             |  measure unitSales(column = "viewTable.unit_sales")
             |
             | table viewTable(sql = $q3 select * from "foodmart"."sales_fact_1998" sf8 join "foodmart"."time_by_day" td on (sf8.time_id = td.time_id) $q3) {
             |    column customer_id
             |    column unit_sales
             |    column the_year
             |    column the_month
             |    column day_of_month
             |    column day_of_week(expression = $q3 extract(day from "the_date")$q3)
             |  }
             |
             |  dimension Customer(table = "viewTable") {
             |    attribute id(column = "customer_id")
             |    attribute year(column = "the_year")
             |    attribute month(column = "the_month")
             |    attribute day(column = "day_of_month")
             |  }
             |}
             |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$3], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Customer.id=[$0], Customer.month=[$1], Customer.day=[$2], unitSales=[$3])
              |    LogicalAggregate(group=[{0, 1, 2}], unitSales=[SUM($3)])
              |      LogicalProject(Customer.id=[$2], Customer.month=[$11], Customer.day=[$13], unitSales=[$7])
              |        LogicalJoin(condition=[=($1, $8)], joinType=[inner])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          JdbcTableScan(table=[[foodmart, time_by_day]])
              |""".stripMargin

          statement.execute(3) mustBe
            """Customer.id=3; Customer.month=April; Customer.day=17; unitSales=16.000
              |Customer.id=3; Customer.month=March; Customer.day=29; unitSales=23.000
              |Customer.id=6; Customer.month=January; Customer.day=16; unitSales=7.000""".stripMargin
        }
      }

      "reserved word in column name" in {
        val dataSources = DsProviders.all

        val schema =
          s"""schema Foodmart (dataSource = "foodmart"){
             | table viewTable(sql = $q3
             |   select 'Terminator' as "character"
             | $q3) {
             |    column character
             |  }
             |
             |  dimension Actor(table = "viewTable") {
             |    attribute character(column = "character")
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(
          rows = List("Actor.character")
        )

        test(schema, dataSources, query) { (statement, entityStatement) =>
          statement.logicalPlan mustBe
            """LogicalProject(Actor.character=['Terminator'])
              |  LogicalValues(tuples=[[{ 0 }]])
              |""".stripMargin

          entityStatement.logicalPlan mustBe
            """LogicalProject(Actor.character=['Terminator'])
              |  LogicalValues(tuples=[[{ 0 }]])
              |""".stripMargin

          statement.execute() mustBe
            """Actor.character=Terminator""".stripMargin

          entityStatement.execute() mustBe
            """Actor.character=Terminator""".stripMargin
        }

      }

    }

    "border cases" should {
      "work with sql reserved words" in {
        val dataSources = DsProviders.all
        val query = AggregateQuery(
          rows = List("cast.cast", "cast.lastName"),
          measures = List("unitSales"),
          orderBy = List(OrderedColumn("cast.cast"), OrderedColumn("cast.lastName"))
        )
        val schema =
          """schema Foodmart {
            |  dimension cast(table = "cast") {
            |    attribute lastName(column = "lname")
            |    attribute cast
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table cast(dataSource = "foodmart", physicalTable="customer") {
            |    column customer_id
            |    column lname
            |    column cast(expression = "total_children")
            |  }
            |
            |  table sales_fact_1998(dataSource = "foodmart") {
            |    column customer_id (tableRef = "cast.customer_id")
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        test(schema, dataSources, query) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(cast.cast=[$0], cast.lastName=[$1], unitSales=[$2])
              |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)])
              |      LogicalProject(cast.cast=[$10], cast.lastName=[$9], unitSales=[$7])
              |        LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          LogicalProject(customer_id=[$0], lname=[$2], cast=[$20])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |""".stripMargin

          statement.execute(6) mustBe
            """cast.cast=0; cast.lastName=Abdulla; unitSales=30.000
              |cast.cast=0; cast.lastName=Ace; unitSales=67.000
              |cast.cast=0; cast.lastName=Adalat; unitSales=18.000
              |cast.cast=0; cast.lastName=Adams; unitSales=222.000
              |cast.cast=0; cast.lastName=Adkinson; unitSales=32.000
              |cast.cast=0; cast.lastName=Agard; unitSales=10.000""".stripMargin
        }
      }

      "schema with multiple datasources" in {
        val query = AggregateQuery(
          rows = List("Store.store.name", "Customer.city"),
          measures = List("sales"),
          orderBy = List(OrderedColumn("Store.store.name"), OrderedColumn("Customer.city"))
        )

        if (!DsProviders.hsqldbFoodmart.isEnabled) {
          cancel("hsqldb should be enabled in test.conf for this test to work")
        }

        val ds1 = DsProviders.defaultDs
        val ds2 = JdbcDataSource(
          "foodmart1",
          "jdbc:hsqldb:res:foodmart",
          Map(
            "user" -> "FOODMART",
            "password" -> "FOODMART"
          ),
          None
        )
        val schema =
          """schema Foodmart(strict = false) {
            |  dimension Store(table = "store") {
            |    level store(column = "store_id") {
            |      attribute name(column = "store_name")
            |      attribute type(column = "store_type")
            |    }
            |  }
            |
            |  dimension Customer(table = "customer") {
            |    level country
            |    level province(column = "state_province")
            |    level city
            |    level customer(column = "customer_id") {
            |      attribute firstName(column = "fname")
            |      attribute lastName(column = "lname")
            |      attribute birthDate(column = "birthdate")
            |      attribute birthYear
            |      attribute gender
            |    }
            |  }
            |
            |  measure sales(column = "sales_fact_1998.store_sales")
            |  measure cost(column = "sales_fact_1998.store_cost")
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table customer(dataSource = "foodmart") {
            |    column customer_id
            |    column country
            |    column state_province
            |    column city
            |    column fname
            |    column lname
            |    column birthdate
            |    column gender
            |    column birthYear(expression = "extract(year from birthdate)")
            |  }
            |
            |  table store(dataSource = "foodmart") {
            |    column store_id
            |    column store_name
            |    column store_type
            |  }
            |
            |  table sales_fact_1998(dataSource = "foodmart1") {
            |    column customer_id (tableRef = "customer.customer_id")
            |    column store_id (tableRef = "store.store_id")
            |
            |    column store_sales
            |    column store_cost
            |    column unit_sales
            |  }
            |}""".stripMargin

        Pantheon.withConnection(new InMemoryCatalog(Map("Foodmart" -> schema), List(ds1, ds2)), "Foodmart") {
          connection =>
            val statement = connection.createStatement(query, Map.empty)
            statement.backendLogicalPlan.toString mustBe
              """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.store.name=[$0], Customer.city=[$1], sales=[$2])
              |    LogicalAggregate(group=[{0, 1}], sales=[SUM($2)])
              |      LogicalProject(Store.store.name=[$40], Customer.city=[$17], sales=[$5])
              |        LogicalJoin(condition=[=($4, $37)], joinType=[left])
              |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart1, sales_fact_1998]])
              |            JdbcTableScan(table=[[foodmart, customer]])
              |          JdbcTableScan(table=[[foodmart, store]])
              |""".stripMargin

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 6) mustBe
                """Store.store.name=Store 1; Customer.city=Acapulco; sales=30838.580
                |Store.store.name=Store 1; Customer.city=La Cruz; sales=18251.450
                |Store.store.name=Store 10; Customer.city=Orizaba; sales=52142.070
                |Store.store.name=Store 11; Customer.city=Beaverton; sales=9705.800
                |Store.store.name=Store 11; Customer.city=Lake Oswego; sales=9129.310
                |Store.store.name=Store 11; Customer.city=Milwaukie; sales=12125.320""".stripMargin
            }
        }
      }
    }

    "SQL queries" should {

      "fail when direct SQL access is not supported" in {

        withDs(DsProviders.all)(ds =>
          Pantheon.withConnection(new InMemoryCatalog(fixture.allSchemas, List(ds)), "Foodmart") { connection =>
            val statement = connection.createStatement(SqlQuery("""Select "gender" from "foodmart"."employee""""))

            intercept[RuntimeException](
              statement.backendLogicalPlan
            ).getMessage mustBe "Table 'foodmart.employee' not found"
        })
      }

      "success when omitting from clause" in {

        withDs(DsProviders.all) { ds =>
          Pantheon.withConnection(new InMemoryCatalog(fixture.allSchemas, List(ds)), "Sales") { connection =>
            val statement = connection.createStatement(SqlQuery("select 1"))

            statement.backendLogicalPlan.toString mustBe
              """LogicalProject(EXPR$0=[1])
                |  LogicalValues(tuples=[[{ 0 }]])
                |""".stripMargin

            statement.backendPhysicalPlan.toString mustBe
              """EnumerableCalc(expr#0=[{inputs}], expr#1=[1], EXPR$0=[$t1])
                |  EnumerableValues(tuples=[[{ 0 }]])
                |""".stripMargin

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 5) mustBe "EXPR$0=1"
            }
          }
        }
      }

      "success when accessing tables defined in schema" in {

        withDs(DsProviders.all) { ds =>
          Pantheon.withConnection(new InMemoryCatalog(fixture.allSchemas, List(ds)), "Sales") { connection =>
            val statement = connection.createStatement(
              SqlQuery("""select s.store_type, r.sales_country
                  |from store s join (select region_id, sales_country from region group by region_id, sales_country) r using (region_id)
                  |group by s.store_type, r.sales_country
                  |order by 1,2
                  |""".stripMargin))

            statement.backendLogicalPlan.toString mustBe
              """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
                |  LogicalAggregate(group=[{0, 1}])
                |    LogicalProject(store_type=[$1], sales_country=[$25])
                |      LogicalJoin(condition=[=($2, $24)], joinType=[inner])
                |        LogicalProject(store_id=[$0], store_type=[$1], region_id=[$2], store_name=[$3], store_number=[$4], store_street_address=[$5], store_city=[$6], store_state=[$7], store_postal_code=[$8], store_country=[$9], store_manager=[$10], store_phone=[$11], store_fax=[$12], first_opened_date=[$13], last_remodel_date=[$14], store_sqft=[$15], grocery_sqft=[$16], frozen_sqft=[$17], meat_sqft=[$18], coffee_bar=[$19], video_store=[$20], salad_bar=[$21], prepared_food=[$22], florist=[$23])
                |          JdbcTableScan(table=[[foodmart, store]])
                |        LogicalAggregate(group=[{0, 1}])
                |          LogicalProject(region_id=[$0], sales_country=[$5])
                |            JdbcTableScan(table=[[foodmart, region]])
                |""".stripMargin

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 5) mustBe
                """store_type=Deluxe Supermarket; sales_country=Canada
                  |store_type=Deluxe Supermarket; sales_country=Mexico
                  |store_type=Deluxe Supermarket; sales_country=USA
                  |store_type=Gourmet Supermarket; sales_country=Mexico
                  |store_type=Gourmet Supermarket; sales_country=USA""".stripMargin
            }
          }
        }
      }

      "success when accessing columns * defined in schema" in {
        val schema = s"""schema Foodmart(dataSource = "foodmart")  {
           |  table sales_fact_1998 {
           |   columns *
           |  }
           |}
           |""".stripMargin

        withDs(DsProviders.all)(ds =>
          Pantheon.withConnection(new InMemoryCatalog(Map("Foodmart" -> schema), List(ds)), "Foodmart") { connection =>
            val statement = connection.createStatement(
              SqlQuery("""select product_id, SUM(store_sales)
                             |from sales_fact_1998
                             |group by product_id
                             |order by 1
                             |""".stripMargin)
            )

            statement.backendLogicalPlan.toString mustBe {
              """LogicalSort(sort0=[$0], dir0=[ASC])
                |  LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])
                |    LogicalProject(product_id=[$0], store_sales=[$5])
                |      JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                |""".stripMargin
            }

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 5) mustBe
                """product_id=1; EXPR$1=421.800
                        |product_id=2; EXPR$1=203.500
                        |product_id=3; EXPR$1=220.780
                        |product_id=4; EXPR$1=1011.920
                        |product_id=5; EXPR$1=611.010""".stripMargin
            }
        })

      }

      def mkSchemaWithViews(columns: String*) =
        s"""schema Foodmart(dataSource = "foodmart")  {
           |
           |  table viewTable(sql = $q3 select * from "foodmart"."sales_fact_1998" sf8 join "foodmart"."time_by_day" td on (sf8.time_id = td.time_id) $q3) {
           |    ${columns.mkString("\n")}
           |  }
           |}
           |""".stripMargin

      "support SQL queries on views" in {
        val schema = mkSchemaWithViews(s"""column day_of_week(expression = $q3 extract(day from "the_date")$q3)""")

        withDs(DsProviders.all)(ds =>
          Pantheon.withConnection(new InMemoryCatalog(Map("Foodmart" -> schema), List(ds)), "Foodmart") { connection =>
            val statement = connection.createStatement(
              SqlQuery("""Select day_of_week from viewTable group by day_of_week order by 1"""))

            statement.backendLogicalPlan.toString mustBe
              """LogicalSort(sort0=[$0], dir0=[ASC])
                |  LogicalAggregate(group=[{0}])
                |    LogicalProject(day_of_week=[EXTRACT(FLAG(DAY), $9)])
                |      LogicalJoin(condition=[=($1, $8)], joinType=[inner])
                |        JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                |        JdbcTableScan(table=[[foodmart, time_by_day]])
                |""".stripMargin

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 5) mustBe
                """day_of_week=1
                  |day_of_week=2
                  |day_of_week=3
                  |day_of_week=4
                  |day_of_week=5""".stripMargin
            }
        })
      }

      "support SQL queries on views with columns*" in {

        // overriding 'customer_id'(exists in db), adding 'day_of_week
        val schema = mkSchemaWithViews("columns *",
                                       """column customer_id(expression = "777")""",
                                       s"""column day_extract(expression = $q3 extract(day from "the_date")$q3)""")

        withDs(DsProviders.all)(ds =>
          Pantheon.withConnection(new InMemoryCatalog(Map("Foodmart" -> schema), List(ds)), "Foodmart") { connection =>
            val statement = connection.createStatement(
              SqlQuery(
                """Select the_month, unit_sales, day_of_month, customer_id, day_extract """ +
                  "from viewTable " +
                  "order by the_month, day_extract, unit_sales desc".stripMargin)
            )

            /* statement created with the following query, fails with exception during execution:

             java.sql.SQLException: exception while executing query: java.sql.SQLSyntaxErrorException: incompatible data type in conversion: from SQL type BIGINT to java.lang.Integer, value: 214963
                 at org.apache.calcite.avatica.Helper.createException(Helper.java:56)
                 at org.apache.calcite.avatica.Helper.createException(Helper.java:41)
                 at org.apache.calcite.avatica.AvaticaConnection.executeQueryInternal(AvaticaConnection.java:540)
                 at org.apache.calcite.avatica.AvaticaPreparedStatement.executeQuery(AvaticaPreparedStatement.java:133)
                 at pantheon.backend.calcite.CalcitePreparedStatement.execute(CalciteConnection.scala:43)

              SqlQuery(
                """Select the_month, SUM(day_of_month), SUM(day_of_week)""" +
                  "from viewTable " +
                  "group by the_month " +
                  "order by the_month".stripMargin)
             */

            statement.backendLogicalPlan.toString mustBe
              """LogicalSort(sort0=[$0], sort1=[$4], sort2=[$1], dir0=[ASC], dir1=[ASC], dir2=[DESC])
                |  LogicalProject(the_month=[$11], unit_sales=[$7], day_of_month=[$13], customer_id=[777], day_extract=[EXTRACT(FLAG(DAY), $9)])
                |    LogicalJoin(condition=[=($1, $8)], joinType=[inner])
                |      JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                |      JdbcTableScan(table=[[foodmart, time_by_day]])
                |""".stripMargin

            withResource(statement.execute()) { rs =>
              RowSetUtil.toString(rs, 5) mustBe
                """the_month=April; unit_sales=3.000; day_of_month=1; customer_id=777; day_extract=1
                  |the_month=April; unit_sales=3.000; day_of_month=1; customer_id=777; day_extract=1
                  |the_month=April; unit_sales=3.000; day_of_month=1; customer_id=777; day_extract=1
                  |the_month=April; unit_sales=2.000; day_of_month=1; customer_id=777; day_extract=1
                  |the_month=April; unit_sales=2.000; day_of_month=1; customer_id=777; day_extract=1""".stripMargin
            }
        })
      }
    }

    "import/export" should {
      "work correctly with imported schemas" in {

        val dataSources = DsProviders.without(mysql)

        val schema =
          """schema Foodmart {
            |  import Sales
            |  import {Inventory => inv}
            |
            |  dimension Store {
            |    conforms Sales.Store
            |    conforms inv.Store
            |  }
            |
            |  measure sales {
            |    conforms Sales.sales
            |  }
            |
            |  measure warehouseSales {
            |    conforms inv.warehouseSales
            |  }
            |}""".stripMargin

        val runTest: PantheonQuery => ((Later[Statement], Later[Statement]) => Unit) => Unit =
          testWithFoodmart(dataSources, _, Some(schema))

        runTest(
          AggregateQuery(
            rows = List("Store.region"),
            measures = List("sales", "warehouseSales"),
            orderBy = List(OrderedColumn("Store.region"), OrderedColumn("sales"))
          )) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region=[CASE(IS NOT NULL($0), $0, $2)], sales=[$1], warehouseSales=[$3])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], sales=[SUM($1)])
              |        LogicalProject(Store.region=[$32], sales=[$5])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0}], warehouseSales=[SUM($1)])
              |        LogicalProject(Store.region=[$34], warehouseSales=[$6])
              |          LogicalJoin(condition=[=($3, $10)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region=1; sales=4230.020; warehouseSales=2057.346
              |Store.region=2; sales=23142.790; warehouseSales=10662.539
              |Store.region=3; sales=50819.150; warehouseSales=23998.142
              |Store.region=4; sales=4328.870; warehouseSales=2065.922
              |Store.region=5; sales=77931.170; warehouseSales=21730.732
              |Store.region=6; sales=20114.290; warehouseSales=9447.087""".stripMargin
        }

        intercept[IncompatibleQueryException](
          testStatement(schema,
                        AggregateQuery(rows = List("Sales.Date.Year"), measures = List("Sales.sales")),
                        fixture.allSchemas)(_ => ())
        ).msg mustBe "dimensions [Sales.Date.Year] not found in schema"

        intercept[IncompatibleQueryException](
          testStatement(schema,
                        AggregateQuery(rows = List("inv.Date.Year"), measures = List("inv.warehouseSales")),
                        fixture.allSchemas)(_ => ())
        ).msg mustBe "dimensions [inv.Date.Year] not found in schema"

      }

      "work correctly with imported measures/dimensions" is (pending)

      "work correctly with wildcard exported measures/dimensions" in {

        val dataSources = DsProviders.without(mysql)

        val schema =
          """schema Foodmart {
            |  export Sales._
            |  export Inventory
            |  dimension ConformingStore {
            |    conforms Store
            |    conforms Inventory.Store
            |  }
            |}""".stripMargin

        val runTest: PantheonQuery => ((Later[Statement], Later[Statement]) => Unit) => Unit =
          testWithFoodmart(dataSources, _, Some(schema))

        runTest(
          AggregateQuery(
            measures = List("Inventory.warehouseSales", "sales"),
            orderBy = List(OrderedColumn("Inventory.warehouseSales"), OrderedColumn("sales"))
          )) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Inventory.warehouseSales=[$1], sales=[$0])
              |    LogicalJoin(condition=[true], joinType=[full])
              |      LogicalAggregate(group=[{}], sales=[SUM($0)])
              |        LogicalProject(sales=[$5])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |      LogicalAggregate(group=[{}], Inventory.warehouseSales=[SUM($0)])
              |        LogicalProject(Inventory.warehouseSales=[$6])
              |          JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |""".stripMargin

          // decimal precision of 2 for ClickHouse
          statement.execute(6, 2) mustBe
            """Inventory.warehouseSales=348484.720; sales=1079147.470""".stripMargin
        }

        runTest(
          AggregateQuery(
            rows = List("Store.region"),
            measures = List("sales"),
            orderBy = List(OrderedColumn("Store.region"), OrderedColumn("sales"))
          )) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Store.region=[$0], sales=[$1])
              |    LogicalAggregate(group=[{0}], sales=[SUM($1)])
              |      LogicalProject(Store.region=[$32], sales=[$5])
              |        LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |          LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, store]])
              |            JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Store.region=1; sales=4230.020
              |Store.region=2; sales=23142.790
              |Store.region=3; sales=50819.150
              |Store.region=4; sales=4328.870
              |Store.region=5; sales=77931.170
              |Store.region=6; sales=20114.290""".stripMargin
        }

        runTest(
          AggregateQuery(
            rows = List("ConformingStore.region"),
            measures = List("sales", "Inventory.warehouseSales"),
            orderBy = List(OrderedColumn("ConformingStore.region"), OrderedColumn("sales"))
          )) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(ConformingStore.region=[CASE(IS NOT NULL($0), $0, $2)], sales=[$1], Inventory.warehouseSales=[$3])
              |    LogicalJoin(condition=[=($0, $2)], joinType=[full])
              |      LogicalAggregate(group=[{0}], sales=[SUM($1)])
              |        LogicalProject(ConformingStore.region=[$32], sales=[$5])
              |          LogicalJoin(condition=[=($4, $8)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |      LogicalAggregate(group=[{0}], Inventory.warehouseSales=[SUM($1)])
              |        LogicalProject(ConformingStore.region=[$34], Inventory.warehouseSales=[$6])
              |          LogicalJoin(condition=[=($3, $10)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |            LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |              JdbcTableScan(table=[[foodmart, store]])
              |              JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """ConformingStore.region=1; sales=4230.020; Inventory.warehouseSales=2057.346
              |ConformingStore.region=2; sales=23142.790; Inventory.warehouseSales=10662.539
              |ConformingStore.region=3; sales=50819.150; Inventory.warehouseSales=23998.142
              |ConformingStore.region=4; sales=4328.870; Inventory.warehouseSales=2065.922
              |ConformingStore.region=5; sales=77931.170; Inventory.warehouseSales=21730.732
              |ConformingStore.region=6; sales=20114.290; Inventory.warehouseSales=9447.087""".stripMargin
        }

        runTest(
          AggregateQuery(
            rows = List("Inventory.Store.region"),
            measures = List("Inventory.warehouseSales"),
            orderBy = List(OrderedColumn("Inventory.Store.region"), OrderedColumn("Inventory.warehouseSales"))
          )) { (statement, _) =>
          statement.logicalPlan mustBe
            """LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
              |  LogicalProject(Inventory.Store.region=[$0], Inventory.warehouseSales=[$1])
              |    LogicalAggregate(group=[{0}], Inventory.warehouseSales=[SUM($1)])
              |      LogicalProject(Inventory.Store.region=[$34], Inventory.warehouseSales=[$6])
              |        LogicalJoin(condition=[=($3, $10)], joinType=[left])
              |          JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
              |          LogicalJoin(condition=[=($2, $24)], joinType=[left])
              |            JdbcTableScan(table=[[foodmart, store]])
              |            JdbcTableScan(table=[[foodmart, region]])
              |""".stripMargin

          statement.execute(6) mustBe
            """Inventory.Store.region=1; Inventory.warehouseSales=2057.346
              |Inventory.Store.region=2; Inventory.warehouseSales=10662.539
              |Inventory.Store.region=3; Inventory.warehouseSales=23998.142
              |Inventory.Store.region=4; Inventory.warehouseSales=2065.922
              |Inventory.Store.region=5; Inventory.warehouseSales=21730.732
              |Inventory.Store.region=6; Inventory.warehouseSales=9447.087""".stripMargin
        }

        runTest(AggregateQuery(rows = List("Inventory.Store.region"), measures = List("sales"))) { (statement, _) =>
          assertThrows[PlannerException] {
            statement.logicalPlan
          }
        }
        intercept[IncompatibleQueryException](
          testStatement(
            schema,
            AggregateQuery(rows = List("Inventory.region"), measures = List("Inventory.warehouseSales")),
            fixture.allSchemas
          )(_ => ())
        ).msg mustBe "dimensions [Inventory.region] not found in schema"
      }

    }

    "filter with nulls" in {

      val dataSources = DsProviders.all
      val psl = s"""schema Foodmart (dataSource = "foodmart", strict = false) {
                   |  dimension Customer(table = "customer") {
                   |    attribute address1 (column = "address1")
                   |    attribute address2 (column = "address2")
                   |  }
                   |}
                   |""".stripMargin

      val q = AggregateQuery(
        rows = List("Customer.address1", "Customer.address2"),
        filter = Some(ref("Customer.address1").isNotNull & ref("Customer.address2").isNull),
        orderBy = List(OrderedColumn("Customer.address1")),
        limit = Some(1)
      )

      test(psl, dataSources, q)((res, _) => {
        res.logicalPlan mustBe """LogicalSort(sort0=[$0], dir0=[ASC], fetch=[1])
                                 |  LogicalProject(Customer.address1=[$0], Customer.address2=[$1])
                                 |    LogicalAggregate(group=[{0, 1}])
                                 |      LogicalProject(Customer.address1=[$5], Customer.address2=[$6])
                                 |        LogicalFilter(condition=[AND(IS NULL($6), IS NOT NULL($5))])
                                 |          JdbcTableScan(table=[[foodmart, customer]])
                                 |""".stripMargin

        res.execute(decimalScale = 2) mustBe "Customer.address1=10 Hillsborough Dr.; Customer.address2=null"
      })
    }

    "Pivoted query" in {

      val dataSources = DsProviders.without(mysql)
      val psl = s"""schema Foodmart (dataSource = "foodmart") {
                    |
                    |  dimension Product(table = "product") {
                    |    attribute id(column = "product_id")
                    |    attribute brand(column = "brand_name")
                    |    attribute name(column = "product_name")
                    |    attribute weight(column = "net_weight")
                    |    attribute recyclable_packaging(column = "recyclable_package")
                    |    attribute low_fat(column = "low_fat")
                    |  }
                    |
                    |  dimension Store(table = "store") {
                    |    level store(column = "store_id") {
                    |      attribute name(column = "store_name")
                    |      attribute type(column = "store_type")
                    |    }
                    |  }
                    |
                    |  filter  "Product.id > 20"
                    |
                    |  dimension X(table = "sales_fact_1998") {
                    |    attribute tid(column = "time_id")
                    |  }
                    |  //Direct parent child relation
                    |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
                    |  measure warehouseSalesx(measure = "warehouseSales", filter =  "Product.weight > 5")
                    |
                    |  //Indirect parent child relation
                    |  measure storeCost(filter =  "X.tid > 1000", column = "sales_fact_1998.store_cost")
                    |  measure storeSales(column = "sales_fact_1998.store_sales")
                    |
                    |  measure calc1 (calculation = "storeSales + 1")
                    |
                    |  table product {
                    |    column product_id
                    |    column brand_name
                    |    column product_name
                    |    column net_weight
                    |    column recyclable_package
                    |    column low_fat
                    |  }
                    |
                    |  table store {
                    |    column store_id
                    |    column store_name
                    |    column store_type
                    |  }
                    |
                    |  table sales_fact_1998 {
                    |    column time_id
                    |    column product_id(tableRef = "product.product_id")
                    |    column store_id(tableRef = "store.store_id")
                    |    column store_sales
                    |    column store_cost
                    |  }
                    |
                    |  table inventory_fact_1998 {
                    |    column product_id(tableRef = "product.product_id")
                    |    column store_id(tableRef = "store.store_id")
                    |    column warehouse_sales
                    |  }
                    |}
                    |""".stripMargin

      val query1 = AggregateQuery(
        filter = Some(ref("Product.name").like(lit("R%"))),
        columns = List("Product.name", "Product.brand"),
        rows = List("Store.store.name", "Store.store.type"),
        // adding customerId to measures in order to test that filer is applied
        measures = List("warehouseSales", "calc1"),
        offset = 10,
        limit = Some(6),
        orderBy = List(OrderedColumn("Store.store.name"))
      )

      test(psl, dataSources, query1)((res, _) => {
                res.logicalPlan mustBe
          """
            |Query for column headers:
            |LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
            |  LogicalProject(Product.name=[CASE(IS NOT NULL($0), $0, $3)], Product.brand=[CASE(IS NOT NULL($1), $1, $4)], warehouseSales=[$5], calc1=[+($2, 1)])
            |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
            |      LogicalAggregate(group=[{0, 1}], storeSales=[SUM($2)])
            |        LogicalProject(Product.name=[$11], Product.brand=[$10], storeSales=[$5])
            |          LogicalFilter(condition=[AND(LIKE($11, 'R%'), >($9, 20))])
            |            LogicalJoin(condition=[=($0, $9)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, sales_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |      LogicalAggregate(group=[{0, 1}], warehouseSales=[SUM($2)])
            |        LogicalProject(Product.name=[$13], Product.brand=[$12], warehouseSales=[$6])
            |          LogicalFilter(condition=[AND(LIKE($13, 'R%'), >($11, 20))])
            |            LogicalJoin(condition=[=($0, $11)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |
            |Query for values:
            |LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[880])
            |  LogicalProject(Store.store.name=[CASE(IS NOT NULL($0), $0, $5)], Store.store.type=[CASE(IS NOT NULL($1), $1, $6)], Product.name=[CASE(IS NOT NULL($2), $2, $7)], Product.brand=[CASE(IS NOT NULL($3), $3, $8)], warehouseSales=[$9], calc1=[+($4, 1)])
            |    LogicalJoin(condition=[AND(=($0, $5), =($1, $6), =($2, $7), =($3, $8))], joinType=[full])
            |      LogicalAggregate(group=[{0, 1, 2, 3}], storeSales=[SUM($4)])
            |        LogicalProject(Store.store.name=[$26], Store.store.type=[$24], Product.name=[$11], Product.brand=[$10], storeSales=[$5])
            |          LogicalFilter(condition=[AND(LIKE($11, 'R%'), >($9, 20))])
            |            LogicalJoin(condition=[=($4, $23)], joinType=[left])
            |              LogicalJoin(condition=[=($0, $9)], joinType=[left])
            |                JdbcTableScan(table=[[foodmart, sales_fact_1998]])
            |                JdbcTableScan(table=[[foodmart, product]])
            |              JdbcTableScan(table=[[foodmart, store]])
            |      LogicalAggregate(group=[{0, 1, 2, 3}], warehouseSales=[SUM($4)])
            |        LogicalProject(Store.store.name=[$28], Store.store.type=[$26], Product.name=[$13], Product.brand=[$12], warehouseSales=[$6])
            |          LogicalFilter(condition=[AND(LIKE($13, 'R%'), >($11, 20))])
            |            LogicalJoin(condition=[=($3, $25)], joinType=[left])
            |              LogicalJoin(condition=[=($0, $11)], joinType=[left])
            |                JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
            |                JdbcTableScan(table=[[foodmart, product]])
            |              JdbcTableScan(table=[[foodmart, store]])
            |""".stripMargin

        res.execute(decimalScale = 2) mustBe
          """Store.store.name=Store 1; Store.store.type=Supermarket; Radius Corn Puffs->Radius->warehouseSales=34.910; Radius Corn Puffs->Radius->calc1=9.280; Radius Grits->Radius->warehouseSales=null; Radius Grits->Radius->calc1=11.320; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=44.010; Radius Wheat Puffs->Radius->warehouseSales=80.940; Radius Wheat Puffs->Radius->calc1=19.480; Red Spade Beef Bologna->Red Spade->warehouseSales=50.760; Red Spade Beef Bologna->Red Spade->calc1=68.680; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=null; Red Spade Chicken Hot Dogs->Red Spade->calc1=6.900; Red Spade Cole Slaw->Red Spade->warehouseSales=8.540; Red Spade Cole Slaw->Red Spade->calc1=22.340; Red Spade Corned Beef->Red Spade->warehouseSales=49.270; Red Spade Corned Beef->Red Spade->calc1=58.540; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=113.560; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=65.080; Red Spade Low Fat Bologna->Red Spade->warehouseSales=null; Red Spade Low Fat Bologna->Red Spade->calc1=21.640; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=67.150; Red Spade Low Fat Cole Slaw->Red Spade->calc1=38.200; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=12.270; Red Spade Potato Salad->Red Spade->warehouseSales=null; Red Spade Potato Salad->Red Spade->calc1=11.880; Red Spade Roasted Chicken->Red Spade->warehouseSales=null; Red Spade Roasted Chicken->Red Spade->calc1=39.640; Red Spade Sliced Chicken->Red Spade->warehouseSales=null; Red Spade Sliced Chicken->Red Spade->calc1=34.800; Red Spade Sliced Ham->Red Spade->warehouseSales=null; Red Spade Sliced Ham->Red Spade->calc1=29.260; Red Spade Sliced Turkey->Red Spade->warehouseSales=16.050; Red Spade Sliced Turkey->Red Spade->calc1=24.300; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=15.880; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 100 Watt Lightbulb->Red Wing->calc1=14.860; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=46.390; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=26.270; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=25.000; Red Wing 75 Watt Lightbulb->Red Wing->calc1=87.800; Red Wing AA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AA-Size Batteries->Red Wing->calc1=71.060; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=56.350; Red Wing Bees Wax Candles->Red Wing->warehouseSales=null; Red Wing Bees Wax Candles->Red Wing->calc1=13.180; Red Wing C-Size Batteries->Red Wing->warehouseSales=null; Red Wing C-Size Batteries->Red Wing->calc1=9.640; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=31.480; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=79.300; Red Wing Counter Cleaner->Red Wing->warehouseSales=23.580; Red Wing Counter Cleaner->Red Wing->calc1=14.020; Red Wing D-Size Batteries->Red Wing->warehouseSales=34.430; Red Wing D-Size Batteries->Red Wing->calc1=25.000; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=13.950; Red Wing Economy Toilet Brush->Red Wing->calc1=16.770; Red Wing Frying Pan->Red Wing->warehouseSales=null; Red Wing Frying Pan->Red Wing->calc1=48.340; Red Wing Glass Cleaner->Red Wing->warehouseSales=null; Red Wing Glass Cleaner->Red Wing->calc1=34.600; Red Wing Large Sponge->Red Wing->warehouseSales=3.880; Red Wing Large Sponge->Red Wing->calc1=10.490; Red Wing Paper Cups->Red Wing->warehouseSales=null; Red Wing Paper Cups->Red Wing->calc1=13.460; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=48.180; Red Wing Paper Towels->Red Wing->warehouseSales=null; Red Wing Paper Towels->Red Wing->calc1=18.920; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=60.570; Red Wing Plastic Knives->Red Wing->warehouseSales=252.110; Red Wing Plastic Knives->Red Wing->calc1=76.000; Red Wing Plastic Spoons->Red Wing->warehouseSales=86.360; Red Wing Plastic Spoons->Red Wing->calc1=25.220; Red Wing Room Freshener->Red Wing->warehouseSales=null; Red Wing Room Freshener->Red Wing->calc1=68.850; Red Wing Scented Tissue->Red Wing->warehouseSales=null; Red Wing Scented Tissue->Red Wing->calc1=20.040; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=77.540; Red Wing Scented Toilet Tissue->Red Wing->calc1=25.920; Red Wing Scissors->Red Wing->warehouseSales=28.410; Red Wing Scissors->Red Wing->calc1=5.920; Red Wing Screw Driver->Red Wing->warehouseSales=205.150; Red Wing Screw Driver->Red Wing->calc1=60.840; Red Wing Silver Cleaner->Red Wing->warehouseSales=22.640; Red Wing Silver Cleaner->Red Wing->calc1=22.930; Red Wing Soft Napkins->Red Wing->warehouseSales=null; Red Wing Soft Napkins->Red Wing->calc1=38.030; Red Wing Tissues->Red Wing->warehouseSales=23.280; Red Wing Tissues->Red Wing->calc1=19.750; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=132.860; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=47.720; Red Wing Toilet Paper->Red Wing->warehouseSales=109.690; Red Wing Toilet Paper->Red Wing->calc1=24.320; Robust Monthly Auto Magazine->Robust->warehouseSales=null; Robust Monthly Auto Magazine->Robust->calc1=11.200; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=18.640; Robust Monthly Fashion Magazine->Robust->warehouseSales=33.600; Robust Monthly Fashion Magazine->Robust->calc1=33.000; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=18.820; Robust Monthly Sports Magazine->Robust->warehouseSales=null; Robust Monthly Sports Magazine->Robust->calc1=38.050
            |Store.store.name=Store 10; Store.store.type=Supermarket; Radius Corn Puffs->Radius->warehouseSales=null; Radius Corn Puffs->Radius->calc1=21.700; Radius Grits->Radius->warehouseSales=15.510; Radius Grits->Radius->calc1=7.880; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=38.950; Radius Wheat Puffs->Radius->warehouseSales=null; Radius Wheat Puffs->Radius->calc1=48.520; Red Spade Beef Bologna->Red Spade->warehouseSales=null; Red Spade Beef Bologna->Red Spade->calc1=31.080; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=72.040; Red Spade Chicken Hot Dogs->Red Spade->calc1=12.800; Red Spade Cole Slaw->Red Spade->warehouseSales=null; Red Spade Cole Slaw->Red Spade->calc1=22.340; Red Spade Corned Beef->Red Spade->warehouseSales=null; Red Spade Corned Beef->Red Spade->calc1=20.180; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=null; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=43.720; Red Spade Low Fat Bologna->Red Spade->warehouseSales=null; Red Spade Low Fat Bologna->Red Spade->calc1=25.080; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=null; Red Spade Low Fat Cole Slaw->Red Spade->calc1=63.000; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=20.320; Red Spade Potato Salad->Red Spade->warehouseSales=null; Red Spade Potato Salad->Red Spade->calc1=13.920; Red Spade Roasted Chicken->Red Spade->warehouseSales=null; Red Spade Roasted Chicken->Red Spade->calc1=45.160; Red Spade Sliced Chicken->Red Spade->warehouseSales=37.890; Red Spade Sliced Chicken->Red Spade->calc1=17.900; Red Spade Sliced Ham->Red Spade->warehouseSales=null; Red Spade Sliced Ham->Red Spade->calc1=21.410; Red Spade Sliced Turkey->Red Spade->warehouseSales=null; Red Spade Sliced Turkey->Red Spade->calc1=17.310; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=51.220; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 100 Watt Lightbulb->Red Wing->calc1=19.480; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=27.700; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=34.250; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=80.580; Red Wing 75 Watt Lightbulb->Red Wing->calc1=38.200; Red Wing AA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AA-Size Batteries->Red Wing->calc1=19.080; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=82.180; Red Wing Bees Wax Candles->Red Wing->warehouseSales=null; Red Wing Bees Wax Candles->Red Wing->calc1=16.660; Red Wing C-Size Batteries->Red Wing->warehouseSales=74.450; Red Wing C-Size Batteries->Red Wing->calc1=26.920; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=46.720; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=24.200; Red Wing Counter Cleaner->Red Wing->warehouseSales=null; Red Wing Counter Cleaner->Red Wing->calc1=22.390; Red Wing D-Size Batteries->Red Wing->warehouseSales=null; Red Wing D-Size Batteries->Red Wing->calc1=19.000; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=null; Red Wing Economy Toilet Brush->Red Wing->calc1=9.300; Red Wing Frying Pan->Red Wing->warehouseSales=41.840; Red Wing Frying Pan->Red Wing->calc1=35.190; Red Wing Glass Cleaner->Red Wing->warehouseSales=null; Red Wing Glass Cleaner->Red Wing->calc1=31.240; Red Wing Large Sponge->Red Wing->warehouseSales=null; Red Wing Large Sponge->Red Wing->calc1=11.950; Red Wing Paper Cups->Red Wing->warehouseSales=75.610; Red Wing Paper Cups->Red Wing->calc1=17.020; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=51.550; Red Wing Paper Towels->Red Wing->warehouseSales=null; Red Wing Paper Towels->Red Wing->calc1=7.400; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=39.850; Red Wing Plastic Knives->Red Wing->warehouseSales=null; Red Wing Plastic Knives->Red Wing->calc1=42.250; Red Wing Plastic Spoons->Red Wing->warehouseSales=null; Red Wing Plastic Spoons->Red Wing->calc1=42.520; Red Wing Room Freshener->Red Wing->warehouseSales=null; Red Wing Room Freshener->Red Wing->calc1=51.150; Red Wing Scented Tissue->Red Wing->warehouseSales=null; Red Wing Scented Tissue->Red Wing->calc1=63.560; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=null; Red Wing Scented Toilet Tissue->Red Wing->calc1=11.680; Red Wing Scissors->Red Wing->warehouseSales=null; Red Wing Scissors->Red Wing->calc1=2.640; Red Wing Screw Driver->Red Wing->warehouseSales=null; Red Wing Screw Driver->Red Wing->calc1=57.320; Red Wing Silver Cleaner->Red Wing->warehouseSales=68.830; Red Wing Silver Cleaner->Red Wing->calc1=31.960; Red Wing Soft Napkins->Red Wing->warehouseSales=9.270; Red Wing Soft Napkins->Red Wing->calc1=5.830; Red Wing Tissues->Red Wing->warehouseSales=null; Red Wing Tissues->Red Wing->calc1=24.750; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=null; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=50.640; Red Wing Toilet Paper->Red Wing->warehouseSales=null; Red Wing Toilet Paper->Red Wing->calc1=20.080; Robust Monthly Auto Magazine->Robust->warehouseSales=null; Robust Monthly Auto Magazine->Robust->calc1=8.200; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=8.560; Robust Monthly Fashion Magazine->Robust->warehouseSales=19.010; Robust Monthly Fashion Magazine->Robust->calc1=33.000; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=40.600; Robust Monthly Sports Magazine->Robust->warehouseSales=36.310; Robust Monthly Sports Magazine->Robust->calc1=45.460
            |Store.store.name=Store 11; Store.store.type=Supermarket; Radius Corn Puffs->Radius->warehouseSales=50.370; Radius Corn Puffs->Radius->calc1=18.940; Radius Grits->Radius->warehouseSales=null; Radius Grits->Radius->calc1=7.020; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=8.590; Radius Wheat Puffs->Radius->warehouseSales=25.660; Radius Wheat Puffs->Radius->calc1=59.080; Red Spade Beef Bologna->Red Spade->warehouseSales=null; Red Spade Beef Bologna->Red Spade->calc1=61.160; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=null; Red Spade Chicken Hot Dogs->Red Spade->calc1=15.750; Red Spade Cole Slaw->Red Spade->warehouseSales=null; Red Spade Cole Slaw->Red Spade->calc1=23.310; Red Spade Corned Beef->Red Spade->warehouseSales=36.330; Red Spade Corned Beef->Red Spade->calc1=58.540; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=20.510; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=54.400; Red Spade Low Fat Bologna->Red Spade->warehouseSales=19.610; Red Spade Low Fat Bologna->Red Spade->calc1=38.840; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=null; Red Spade Low Fat Cole Slaw->Red Spade->calc1=28.900; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=20.320; Red Spade Potato Salad->Red Spade->warehouseSales=null; Red Spade Potato Salad->Red Spade->calc1=7.800; Red Spade Roasted Chicken->Red Spade->warehouseSales=113.160; Red Spade Roasted Chicken->Red Spade->calc1=67.240; Red Spade Sliced Chicken->Red Spade->warehouseSales=56.240; Red Spade Sliced Chicken->Red Spade->calc1=17.900; Red Spade Sliced Ham->Red Spade->warehouseSales=65.480; Red Spade Sliced Ham->Red Spade->calc1=19.840; Red Spade Sliced Turkey->Red Spade->warehouseSales=null; Red Spade Sliced Turkey->Red Spade->calc1=17.310; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=8.440; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 100 Watt Lightbulb->Red Wing->calc1=30.260; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=70.420; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=32.920; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 75 Watt Lightbulb->Red Wing->calc1=40.680; Red Wing AA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AA-Size Batteries->Red Wing->calc1=39.420; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=96.940; Red Wing Bees Wax Candles->Red Wing->warehouseSales=14.130; Red Wing Bees Wax Candles->Red Wing->calc1=41.020; Red Wing C-Size Batteries->Red Wing->warehouseSales=null; Red Wing C-Size Batteries->Red Wing->calc1=38.440; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=61.960; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=76.400; Red Wing Counter Cleaner->Red Wing->warehouseSales=null; Red Wing Counter Cleaner->Red Wing->calc1=26.110; Red Wing D-Size Batteries->Red Wing->warehouseSales=null; Red Wing D-Size Batteries->Red Wing->calc1=25.000; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=null; Red Wing Economy Toilet Brush->Red Wing->calc1=19.260; Red Wing Frying Pan->Red Wing->warehouseSales=null; Red Wing Frying Pan->Red Wing->calc1=48.340; Red Wing Glass Cleaner->Red Wing->warehouseSales=104.190; Red Wing Glass Cleaner->Red Wing->calc1=48.040; Red Wing Large Sponge->Red Wing->warehouseSales=null; Red Wing Large Sponge->Red Wing->calc1=15.600; Red Wing Paper Cups->Red Wing->warehouseSales=null; Red Wing Paper Cups->Red Wing->calc1=11.680; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=51.550; Red Wing Paper Towels->Red Wing->warehouseSales=null; Red Wing Paper Towels->Red Wing->calc1=8.040; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=83.880; Red Wing Plastic Knives->Red Wing->warehouseSales=null; Red Wing Plastic Knives->Red Wing->calc1=83.500; Red Wing Plastic Spoons->Red Wing->warehouseSales=18.170; Red Wing Plastic Spoons->Red Wing->calc1=32.140; Red Wing Room Freshener->Red Wing->warehouseSales=144.550; Red Wing Room Freshener->Red Wing->calc1=80.650; Red Wing Scented Tissue->Red Wing->warehouseSales=null; Red Wing Scented Tissue->Red Wing->calc1=49.960; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=null; Red Wing Scented Toilet Tissue->Red Wing->calc1=90.000; Red Wing Scissors->Red Wing->warehouseSales=null; Red Wing Scissors->Red Wing->calc1=19.860; Red Wing Screw Driver->Red Wing->warehouseSales=143.900; Red Wing Screw Driver->Red Wing->calc1=89.000; Red Wing Silver Cleaner->Red Wing->warehouseSales=null; Red Wing Silver Cleaner->Red Wing->calc1=24.220; Red Wing Soft Napkins->Red Wing->warehouseSales=null; Red Wing Soft Napkins->Red Wing->calc1=52.520; Red Wing Tissues->Red Wing->warehouseSales=null; Red Wing Tissues->Red Wing->calc1=22.250; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=null; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=12.680; Red Wing Toilet Paper->Red Wing->warehouseSales=null; Red Wing Toilet Paper->Red Wing->calc1=9.480; Robust Monthly Auto Magazine->Robust->warehouseSales=18.380; Robust Monthly Auto Magazine->Robust->calc1=17.800; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=18.640; Robust Monthly Fashion Magazine->Robust->warehouseSales=null; Robust Monthly Fashion Magazine->Robust->calc1=55.400; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=40.600; Robust Monthly Sports Magazine->Robust->warehouseSales=null; Robust Monthly Sports Magazine->Robust->calc1=13.350
            |Store.store.name=Store 12; Store.store.type=Deluxe Supermarket; Radius Corn Puffs->Radius->warehouseSales=null; Radius Corn Puffs->Radius->calc1=54.820; Radius Grits->Radius->warehouseSales=18.160; Radius Grits->Radius->calc1=15.620; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=56.660; Radius Wheat Puffs->Radius->warehouseSales=62.730; Radius Wheat Puffs->Radius->calc1=27.400; Red Spade Beef Bologna->Red Spade->warehouseSales=null; Red Spade Beef Bologna->Red Spade->calc1=110.040; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=null; Red Spade Chicken Hot Dogs->Red Spade->calc1=68.850; Red Spade Cole Slaw->Red Spade->warehouseSales=null; Red Spade Cole Slaw->Red Spade->calc1=26.220; Red Spade Corned Beef->Red Spade->warehouseSales=null; Red Spade Corned Beef->Red Spade->calc1=69.500; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=null; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=97.120; Red Spade Low Fat Bologna->Red Spade->warehouseSales=null; Red Spade Low Fat Bologna->Red Spade->calc1=90.440; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=null; Red Spade Low Fat Cole Slaw->Red Spade->calc1=106.400; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=13.880; Red Spade Potato Salad->Red Spade->warehouseSales=null; Red Spade Potato Salad->Red Spade->calc1=21.400; Red Spade Roasted Chicken->Red Spade->warehouseSales=null; Red Spade Roasted Chicken->Red Spade->calc1=70.000; Red Spade Sliced Chicken->Red Spade->warehouseSales=null; Red Spade Sliced Chicken->Red Spade->calc1=24.660; Red Spade Sliced Ham->Red Spade->warehouseSales=null; Red Spade Sliced Ham->Red Spade->calc1=33.970; Red Spade Sliced Turkey->Red Spade->warehouseSales=null; Red Spade Sliced Turkey->Red Spade->calc1=59.250; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=27.040; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 100 Watt Lightbulb->Red Wing->calc1=30.260; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=78.430; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=54.200; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=1.090; Red Wing 75 Watt Lightbulb->Red Wing->calc1=58.040; Red Wing AA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AA-Size Batteries->Red Wing->calc1=86.880; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=71.110; Red Wing Bees Wax Candles->Red Wing->warehouseSales=null; Red Wing Bees Wax Candles->Red Wing->calc1=58.420; Red Wing C-Size Batteries->Red Wing->warehouseSales=null; Red Wing C-Size Batteries->Red Wing->calc1=81.640; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=81.010; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=82.200; Red Wing Counter Cleaner->Red Wing->warehouseSales=null; Red Wing Counter Cleaner->Red Wing->calc1=19.600; Red Wing D-Size Batteries->Red Wing->warehouseSales=null; Red Wing D-Size Batteries->Red Wing->calc1=40.000; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=null; Red Wing Economy Toilet Brush->Red Wing->calc1=27.560; Red Wing Frying Pan->Red Wing->warehouseSales=null; Red Wing Frying Pan->Red Wing->calc1=32.560; Red Wing Glass Cleaner->Red Wing->warehouseSales=null; Red Wing Glass Cleaner->Red Wing->calc1=53.080; Red Wing Large Sponge->Red Wing->warehouseSales=null; Red Wing Large Sponge->Red Wing->calc1=23.630; Red Wing Paper Cups->Red Wing->warehouseSales=null; Red Wing Paper Cups->Red Wing->calc1=27.700; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=118.950; Red Wing Paper Towels->Red Wing->warehouseSales=null; Red Wing Paper Towels->Red Wing->calc1=10.600; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=83.880; Red Wing Plastic Knives->Red Wing->warehouseSales=null; Red Wing Plastic Knives->Red Wing->calc1=79.750; Red Wing Plastic Spoons->Red Wing->warehouseSales=null; Red Wing Plastic Spoons->Red Wing->calc1=139.400; Red Wing Room Freshener->Red Wing->warehouseSales=null; Red Wing Room Freshener->Red Wing->calc1=36.400; Red Wing Scented Tissue->Red Wing->warehouseSales=null; Red Wing Scented Tissue->Red Wing->calc1=47.240; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=null; Red Wing Scented Toilet Tissue->Red Wing->calc1=97.120; Red Wing Scissors->Red Wing->warehouseSales=null; Red Wing Scissors->Red Wing->calc1=20.680; Red Wing Screw Driver->Red Wing->warehouseSales=null; Red Wing Screw Driver->Red Wing->calc1=81.960; Red Wing Silver Cleaner->Red Wing->warehouseSales=null; Red Wing Silver Cleaner->Red Wing->calc1=24.220; Red Wing Soft Napkins->Red Wing->warehouseSales=null; Red Wing Soft Napkins->Red Wing->calc1=34.810; Red Wing Tissues->Red Wing->warehouseSales=null; Red Wing Tissues->Red Wing->calc1=36.000; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=null; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=53.560; Red Wing Toilet Paper->Red Wing->warehouseSales=null; Red Wing Toilet Paper->Red Wing->calc1=47.640; Robust Monthly Auto Magazine->Robust->warehouseSales=null; Robust Monthly Auto Magazine->Robust->calc1=8.800; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=50.140; Robust Monthly Fashion Magazine->Robust->warehouseSales=null; Robust Monthly Fashion Magazine->Robust->calc1=33.000; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=50.500; Robust Monthly Sports Magazine->Robust->warehouseSales=null; Robust Monthly Sports Magazine->Robust->calc1=42.990
            |Store.store.name=Store 13; Store.store.type=Deluxe Supermarket; Radius Corn Puffs->Radius->warehouseSales=null; Radius Corn Puffs->Radius->calc1=38.260; Radius Grits->Radius->warehouseSales=48.160; Radius Grits->Radius->calc1=25.940; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=8.590; Radius Wheat Puffs->Radius->warehouseSales=31.920; Radius Wheat Puffs->Radius->calc1=43.240; Red Spade Beef Bologna->Red Spade->warehouseSales=36.850; Red Spade Beef Bologna->Red Spade->calc1=76.200; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=null; Red Spade Chicken Hot Dogs->Red Spade->calc1=48.200; Red Spade Cole Slaw->Red Spade->warehouseSales=null; Red Spade Cole Slaw->Red Spade->calc1=17.490; Red Spade Corned Beef->Red Spade->warehouseSales=null; Red Spade Corned Beef->Red Spade->calc1=39.360; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=212.320; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=43.720; Red Spade Low Fat Bologna->Red Spade->warehouseSales=51.880; Red Spade Low Fat Bologna->Red Spade->calc1=104.200; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=null; Red Spade Low Fat Cole Slaw->Red Spade->calc1=109.500; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=46.080; Red Spade Potato Salad->Red Spade->warehouseSales=36.860; Red Spade Potato Salad->Red Spade->calc1=20.040; Red Spade Roasted Chicken->Red Spade->warehouseSales=null; Red Spade Roasted Chicken->Red Spade->calc1=75.520; Red Spade Sliced Chicken->Red Spade->warehouseSales=23.560; Red Spade Sliced Chicken->Red Spade->calc1=44.940; Red Spade Sliced Ham->Red Spade->warehouseSales=null; Red Spade Sliced Ham->Red Spade->calc1=35.540; Red Spade Sliced Turkey->Red Spade->warehouseSales=16.640; Red Spade Sliced Turkey->Red Spade->calc1=91.870; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=45.640; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=10.900; Red Wing 100 Watt Lightbulb->Red Wing->calc1=59.520; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=89.110; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=22.280; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=97.220; Red Wing 75 Watt Lightbulb->Red Wing->calc1=70.440; Red Wing AA-Size Batteries->Red Wing->warehouseSales=84.520; Red Wing AA-Size Batteries->Red Wing->calc1=25.860; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=78.490; Red Wing Bees Wax Candles->Red Wing->warehouseSales=69.550; Red Wing Bees Wax Candles->Red Wing->calc1=18.400; Red Wing C-Size Batteries->Red Wing->warehouseSales=86.110; Red Wing C-Size Batteries->Red Wing->calc1=81.640; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=168.640; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=76.400; Red Wing Counter Cleaner->Red Wing->warehouseSales=null; Red Wing Counter Cleaner->Red Wing->calc1=27.970; Red Wing D-Size Batteries->Red Wing->warehouseSales=null; Red Wing D-Size Batteries->Red Wing->calc1=40.000; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=1.740; Red Wing Economy Toilet Brush->Red Wing->calc1=23.410; Red Wing Frying Pan->Red Wing->warehouseSales=6.970; Red Wing Frying Pan->Red Wing->calc1=53.600; Red Wing Glass Cleaner->Red Wing->warehouseSales=null; Red Wing Glass Cleaner->Red Wing->calc1=54.760; Red Wing Large Sponge->Red Wing->warehouseSales=null; Red Wing Large Sponge->Red Wing->calc1=26.550; Red Wing Paper Cups->Red Wing->warehouseSales=19.100; Red Wing Paper Cups->Red Wing->calc1=50.840; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=81.880; Red Wing Paper Towels->Red Wing->warehouseSales=3.800; Red Wing Paper Towels->Red Wing->calc1=13.160; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=161.580; Red Wing Plastic Knives->Red Wing->warehouseSales=null; Red Wing Plastic Knives->Red Wing->calc1=53.500; Red Wing Plastic Spoons->Red Wing->warehouseSales=null; Red Wing Plastic Spoons->Red Wing->calc1=84.040; Red Wing Room Freshener->Red Wing->warehouseSales=84.960; Red Wing Room Freshener->Red Wing->calc1=42.300; Red Wing Scented Tissue->Red Wing->warehouseSales=15.230; Red Wing Scented Tissue->Red Wing->calc1=58.120; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=null; Red Wing Scented Toilet Tissue->Red Wing->calc1=47.280; Red Wing Scissors->Red Wing->warehouseSales=34.240; Red Wing Scissors->Red Wing->calc1=14.940; Red Wing Screw Driver->Red Wing->warehouseSales=151.780; Red Wing Screw Driver->Red Wing->calc1=67.880; Red Wing Silver Cleaner->Red Wing->warehouseSales=62.110; Red Wing Silver Cleaner->Red Wing->calc1=25.510; Red Wing Soft Napkins->Red Wing->warehouseSales=null; Red Wing Soft Napkins->Red Wing->calc1=42.860; Red Wing Tissues->Red Wing->warehouseSales=null; Red Wing Tissues->Red Wing->calc1=37.250; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=null; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=47.720; Red Wing Toilet Paper->Red Wing->warehouseSales=88.790; Red Wing Toilet Paper->Red Wing->calc1=34.920; Robust Monthly Auto Magazine->Robust->warehouseSales=null; Robust Monthly Auto Magazine->Robust->calc1=10.600; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=9.820; Robust Monthly Fashion Magazine->Robust->warehouseSales=null; Robust Monthly Fashion Magazine->Robust->calc1=61.800; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=50.500; Robust Monthly Sports Magazine->Robust->warehouseSales=null; Robust Monthly Sports Magazine->Robust->calc1=35.580
            |Store.store.name=Store 14; Store.store.type=Small Grocery; Radius Corn Puffs->Radius->warehouseSales=null; Radius Corn Puffs->Radius->calc1=5.140; Radius Grits->Radius->warehouseSales=null; Radius Grits->Radius->calc1=1.860; Radius Oatmeal->Radius->warehouseSales=null; Radius Oatmeal->Radius->calc1=8.590; Radius Wheat Puffs->Radius->warehouseSales=null; Radius Wheat Puffs->Radius->calc1=null; Red Spade Beef Bologna->Red Spade->warehouseSales=null; Red Spade Beef Bologna->Red Spade->calc1=null; Red Spade Chicken Hot Dogs->Red Spade->warehouseSales=null; Red Spade Chicken Hot Dogs->Red Spade->calc1=3.950; Red Spade Cole Slaw->Red Spade->warehouseSales=null; Red Spade Cole Slaw->Red Spade->calc1=2.940; Red Spade Corned Beef->Red Spade->warehouseSales=null; Red Spade Corned Beef->Red Spade->calc1=null; Red Spade Foot-Long Hot Dogs->Red Spade->warehouseSales=null; Red Spade Foot-Long Hot Dogs->Red Spade->calc1=18.800; Red Spade Low Fat Bologna->Red Spade->warehouseSales=null; Red Spade Low Fat Bologna->Red Spade->calc1=null; Red Spade Low Fat Cole Slaw->Red Spade->warehouseSales=null; Red Spade Low Fat Cole Slaw->Red Spade->calc1=null; Red Spade Pimento Loaf->Red Spade->warehouseSales=null; Red Spade Pimento Loaf->Red Spade->calc1=null; Red Spade Potato Salad->Red Spade->warehouseSales=null; Red Spade Potato Salad->Red Spade->calc1=null; Red Spade Roasted Chicken->Red Spade->warehouseSales=null; Red Spade Roasted Chicken->Red Spade->calc1=6.520; Red Spade Sliced Chicken->Red Spade->warehouseSales=null; Red Spade Sliced Chicken->Red Spade->calc1=2.690; Red Spade Sliced Ham->Red Spade->warehouseSales=null; Red Spade Sliced Ham->Red Spade->calc1=7.280; Red Spade Sliced Turkey->Red Spade->warehouseSales=null; Red Spade Sliced Turkey->Red Spade->calc1=null; Red Spade Turkey Hot Dogs->Red Spade->warehouseSales=null; Red Spade Turkey Hot Dogs->Red Spade->calc1=4.720; Red Wing 100 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 100 Watt Lightbulb->Red Wing->calc1=5.620; Red Wing 25 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 25 Watt Lightbulb->Red Wing->calc1=9.010; Red Wing 60 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 60 Watt Lightbulb->Red Wing->calc1=null; Red Wing 75 Watt Lightbulb->Red Wing->warehouseSales=null; Red Wing 75 Watt Lightbulb->Red Wing->calc1=5.960; Red Wing AA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AA-Size Batteries->Red Wing->calc1=7.780; Red Wing AAA-Size Batteries->Red Wing->warehouseSales=null; Red Wing AAA-Size Batteries->Red Wing->calc1=null; Red Wing Bees Wax Candles->Red Wing->warehouseSales=null; Red Wing Bees Wax Candles->Red Wing->calc1=null; Red Wing C-Size Batteries->Red Wing->warehouseSales=null; Red Wing C-Size Batteries->Red Wing->calc1=null; Red Wing Copper Cleaner->Red Wing->warehouseSales=null; Red Wing Copper Cleaner->Red Wing->calc1=null; Red Wing Copper Pot Scrubber->Red Wing->warehouseSales=null; Red Wing Copper Pot Scrubber->Red Wing->calc1=null; Red Wing Counter Cleaner->Red Wing->warehouseSales=null; Red Wing Counter Cleaner->Red Wing->calc1=5.650; Red Wing D-Size Batteries->Red Wing->warehouseSales=null; Red Wing D-Size Batteries->Red Wing->calc1=null; Red Wing Economy Toilet Brush->Red Wing->warehouseSales=null; Red Wing Economy Toilet Brush->Red Wing->calc1=1.830; Red Wing Frying Pan->Red Wing->warehouseSales=null; Red Wing Frying Pan->Red Wing->calc1=null; Red Wing Glass Cleaner->Red Wing->warehouseSales=null; Red Wing Glass Cleaner->Red Wing->calc1=6.040; Red Wing Large Sponge->Red Wing->warehouseSales=null; Red Wing Large Sponge->Red Wing->calc1=1.730; Red Wing Paper Cups->Red Wing->warehouseSales=33.290; Red Wing Paper Cups->Red Wing->calc1=4.560; Red Wing Paper Plates->Red Wing->warehouseSales=null; Red Wing Paper Plates->Red Wing->calc1=null; Red Wing Paper Towels->Red Wing->warehouseSales=null; Red Wing Paper Towels->Red Wing->calc1=null; Red Wing Plastic Forks->Red Wing->warehouseSales=null; Red Wing Plastic Forks->Red Wing->calc1=null; Red Wing Plastic Knives->Red Wing->warehouseSales=null; Red Wing Plastic Knives->Red Wing->calc1=4.750; Red Wing Plastic Spoons->Red Wing->warehouseSales=null; Red Wing Plastic Spoons->Red Wing->calc1=7.920; Red Wing Room Freshener->Red Wing->warehouseSales=null; Red Wing Room Freshener->Red Wing->calc1=3.950; Red Wing Scented Tissue->Red Wing->warehouseSales=null; Red Wing Scented Tissue->Red Wing->calc1=3.720; Red Wing Scented Toilet Tissue->Red Wing->warehouseSales=null; Red Wing Scented Toilet Tissue->Red Wing->calc1=15.240; Red Wing Scissors->Red Wing->warehouseSales=null; Red Wing Scissors->Red Wing->calc1=3.460; Red Wing Screw Driver->Red Wing->warehouseSales=null; Red Wing Screw Driver->Red Wing->calc1=22.120; Red Wing Silver Cleaner->Red Wing->warehouseSales=null; Red Wing Silver Cleaner->Red Wing->calc1=2.290; Red Wing Soft Napkins->Red Wing->warehouseSales=null; Red Wing Soft Napkins->Red Wing->calc1=null; Red Wing Tissues->Red Wing->warehouseSales=null; Red Wing Tissues->Red Wing->calc1=3.500; Red Wing Toilet Bowl Cleaner->Red Wing->warehouseSales=null; Red Wing Toilet Bowl Cleaner->Red Wing->calc1=6.840; Red Wing Toilet Paper->Red Wing->warehouseSales=null; Red Wing Toilet Paper->Red Wing->calc1=5.240; Robust Monthly Auto Magazine->Robust->warehouseSales=null; Robust Monthly Auto Magazine->Robust->calc1=2.800; Robust Monthly Computer Magazine->Robust->warehouseSales=null; Robust Monthly Computer Magazine->Robust->calc1=3.520; Robust Monthly Fashion Magazine->Robust->warehouseSales=null; Robust Monthly Fashion Magazine->Robust->calc1=null; Robust Monthly Home Magazine->Robust->warehouseSales=null; Robust Monthly Home Magazine->Robust->calc1=null; Robust Monthly Sports Magazine->Robust->warehouseSales=null; Robust Monthly Sports Magazine->Robust->calc1=5.940""".stripMargin
      })

      // TODO: Columns only query produces wrong plan (containing duplicate field names). This happens when there are measures in 2 fact tables
      val queryWhoutRows = {
        AggregateQuery(
          rows = Nil,
          measures = query1.measures,
          filter = query1.filter,
          offset = query1.offset,
          limit = query1.limit,
          orderBy = Nil,
          columns = query1.columns
        )
      }

      test(psl, dataSources, queryWhoutRows) { (res, _) =>
        res.logicalPlan mustBe
          """
            |Query for column headers:
            |LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
            |  LogicalProject(Product.name=[CASE(IS NOT NULL($0), $0, $3)], Product.brand=[CASE(IS NOT NULL($1), $1, $4)], warehouseSales=[$5], calc1=[+($2, 1)])
            |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
            |      LogicalAggregate(group=[{0, 1}], storeSales=[SUM($2)])
            |        LogicalProject(Product.name=[$11], Product.brand=[$10], storeSales=[$5])
            |          LogicalFilter(condition=[AND(LIKE($11, 'R%'), >($9, 20))])
            |            LogicalJoin(condition=[=($0, $9)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, sales_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |      LogicalAggregate(group=[{0, 1}], warehouseSales=[SUM($2)])
            |        LogicalProject(Product.name=[$13], Product.brand=[$12], warehouseSales=[$6])
            |          LogicalFilter(condition=[AND(LIKE($13, 'R%'), >($11, 20))])
            |            LogicalJoin(condition=[=($0, $11)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |
            |Query for values:
            |LogicalSort(fetch=[880])
            |  LogicalProject(Product.name=[CASE(IS NOT NULL($0), $0, $3)], Product.brand=[CASE(IS NOT NULL($1), $1, $4)], warehouseSales=[$5], calc1=[+($2, 1)])
            |    LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[full])
            |      LogicalAggregate(group=[{0, 1}], storeSales=[SUM($2)])
            |        LogicalProject(Product.name=[$11], Product.brand=[$10], storeSales=[$5])
            |          LogicalFilter(condition=[AND(LIKE($11, 'R%'), >($9, 20))])
            |            LogicalJoin(condition=[=($0, $9)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, sales_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |      LogicalAggregate(group=[{0, 1}], warehouseSales=[SUM($2)])
            |        LogicalProject(Product.name=[$13], Product.brand=[$12], warehouseSales=[$6])
            |          LogicalFilter(condition=[AND(LIKE($13, 'R%'), >($11, 20))])
            |            LogicalJoin(condition=[=($0, $11)], joinType=[left])
            |              JdbcTableScan(table=[[foodmart, inventory_fact_1998]])
            |              JdbcTableScan(table=[[foodmart, product]])
            |""".stripMargin

      // TODO: Fails with calcite exception
//      res.execute() mustBe
//               """Ira->Trujillo->customerId=10240; Ira->Trujillo->unitSales=214.000; Ira->Trujillo->calc1=657.852; Irene->Carr->customerId=4761; Irene->Carr->unitSales=369.000; Irene->Carr->calc1=934.770; Iris->Smith->customerId=4403; Iris->Smith->unitSales=57.000; Iris->Smith->calc1=895.119; Irma->Johnson->customerId=8234; Irma->Johnson->unitSales=45.000; Irma->Johnson->calc1=null; Irvin->Setzer->customerId=4250; Irvin->Setzer->unitSales=362.000; Irvin->Setzer->calc1=892.423; Irving->Buck->customerId=4459; Irving->Buck->unitSales=42.000; Irving->Buck->calc1=884.840""".stripMargin.stripMargin
      }
    }

    "Pivoted query with duplicate fields" in {

      val dataSources = DsProviders.all

      val query = AggregateQuery(
        measures = List("unitSales"),
        columns = List("Customer.fname", "Customer.fname"),
        rows = List("Customer.lname", "Customer.lname"),
        filter = Some(ref("Customer.fname") === lit("Zelah"))
      )

      val schema =
        """schema Foodmart (dataSource = "foodmart", strict = false) {
          |  measure unitSales(column = "sales_fact_1998.unit_sales")
          |
          |  dimension Customer (table = "customer") {
          |    attribute lname (column = "lname")
          |    attribute fname (column = "fname")
          |  }
          |  table customer{
          |    column customer_id
          |  }
          |  table sales_fact_1998 {
          |    column customer_id(tableRef = "customer.customer_id")
          |    column unit_sales
          |  }
          |}
        """.stripMargin

      test(schema, dataSources, query) { (statement, _) =>
        statement.logicalPlan mustBe """
                                       |Query for column headers:
                                       |LogicalSort(sort0=[$0], dir0=[ASC])
                                       |  LogicalProject(Customer.fname=[$0], Customer.fname0=[$0], unitSales=[$1])
                                       |    LogicalAggregate(group=[{0}], unitSales=[SUM($1)])
                                       |      LogicalProject(Customer.fname=[$11], unitSales=[$7])
                                       |        LogicalFilter(condition=[=($11, 'Zelah')])
                                       |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
                                       |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                                       |            JdbcTableScan(table=[[foodmart, customer]])
                                       |
                                       |Query for values:
                                       |LogicalSort(sort0=[$0], dir0=[ASC])
                                       |  LogicalProject(Customer.lname=[$0], Customer.lname0=[$0], Customer.fname=[$1], Customer.fname0=[$1], unitSales=[$2])
                                       |    LogicalAggregate(group=[{0, 1}], unitSales=[SUM($2)])
                                       |      LogicalProject(Customer.lname=[$10], Customer.fname=[$11], unitSales=[$7])
                                       |        LogicalFilter(condition=[=($11, 'Zelah')])
                                       |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
                                       |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                                       |            JdbcTableScan(table=[[foodmart, customer]])
                                       |""".stripMargin

        statement.execute() mustBe "Customer.lname=Burror; Customer.lname0=Burror; Zelah->Zelah->unitSales=72.000"
      }
    }

    "Pivoted query with nullable fields" in {
      val dataSources = DsProviders.all
      val psl = s"""schema Foodmart (dataSource = "foodmart", strict = false) {
                   |  dimension Customer(table = "customer") {
                   |    attribute address1 (column = "address1")
                   |    attribute address2 (column = "address2")
                   |    attribute address3 (column = "address3")
                   |  }
                   |  measure storeSales(column = "sales_fact_1998.store_sales")
                   |
                   |  table sales_fact_1998 {
                   |    column customer_id(tableRef = "customer.customer_id")
                   |    column store_sales
                   |  }
                   |}
                   |""".stripMargin

      val q = AggregateQuery(
        measures = List("storeSales"),
        rows = List("Customer.address1", "Customer.address3"),
        columns = List("Customer.address2"),
        filter =
          Some(ref("Customer.address1").isNotNull & ref("Customer.address2").isNull & ref("Customer.address3").isNull),
        orderBy = List(OrderedColumn("Customer.address1")),
        limit = Some(1)
      )

      test(psl, dataSources, q)((res, _) => {
        res.logicalPlan mustBe """
                                 |Query for column headers:
                                 |LogicalSort(sort0=[$0], dir0=[ASC])
                                 |  LogicalProject(Customer.address2=[$0], storeSales=[$1])
                                 |    LogicalAggregate(group=[{0}], storeSales=[SUM($1)])
                                 |      LogicalProject(Customer.address2=[$14], storeSales=[$5])
                                 |        LogicalFilter(condition=[AND(IS NULL($14), IS NULL($15), IS NOT NULL($13))])
                                 |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
                                 |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                                 |            JdbcTableScan(table=[[foodmart, customer]])
                                 |
                                 |Query for values:
                                 |LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[1])
                                 |  LogicalProject(Customer.address1=[$0], Customer.address3=[$1], Customer.address2=[$2], storeSales=[$3])
                                 |    LogicalAggregate(group=[{0, 1, 2}], storeSales=[SUM($3)])
                                 |      LogicalProject(Customer.address1=[$13], Customer.address3=[$15], Customer.address2=[$14], storeSales=[$5])
                                 |        LogicalFilter(condition=[AND(IS NULL($14), IS NULL($15), IS NOT NULL($13))])
                                 |          LogicalJoin(condition=[=($2, $8)], joinType=[left])
                                 |            JdbcTableScan(table=[[foodmart, sales_fact_1998]])
                                 |            JdbcTableScan(table=[[foodmart, customer]])
                                 |""".stripMargin

        res.execute(decimalScale = 2) mustBe "Customer.address1=10 Hillsborough Dr.; Customer.address3=null; null->storeSales=132.180"
      })
    }

    "compatibility query" should {
      "succeed in case of simple schema with single dimension" in {
        val query = AggregateQuery(
          rows = List("Store.name")
        )

        val ds = DsProviders.defaultDs
        val schema =
          """schema Foodmart(dataSource="foodmart", strict = false) {
            |  dimension Store(table = "store") {
            |    attribute name(column = "store_name")
            |    attribute type(column = "store_type")
            |  }
            |}""".stripMargin

        Pantheon.withConnection(new InMemoryCatalog(Map("Foodmart" -> schema), List(ds)), "Foodmart") { connection =>
          getFields(connection.schema.getCompatibility(query)) mustBe Set("Store.name", "Store.type")
        }
      }

      "succeed for the case with no filtered or calculated measures" in {
        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )

        val ds = DsProviders.defaultDs

        Pantheon.withConnection(new InMemoryCatalog(fixture.allSchemas, List(ds)), "Foodmart") { connection =>
          getFields(connection.schema.getCompatibility(query)) mustBe Set(
            "Store.region.province",
            "Store.region",
            "Store.region.city",
            "Store.store",
            "Store.store.name",
            "Store.region.region",
            "Date.Year",
            "Date.Month",
            "Date.Date",
            "Store.region.country",
            "Store.store.type",
            "sales.unitSales",
            "warehouseSales",
            "sales",
            "sales.sales",
            "inv.warehouseSales",
            "sales.cost",
            "inv.warehouseCost"
          )
        }

      }

      "return correct results with constraints" in {

        val dataSources = DsProviders.all

        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )

        withDs(dataSources)(ds =>
          Pantheon.withConnection(new InMemoryCatalog(fixture.allSchemas, List(ds)), "Foodmart") { connection =>
            getFields(connection.schema.getCompatibility(query, List("Store.store.", "Date.", "inv."))) mustBe Set(
              "Store.store.name",
              "Date.Year",
              "Date.Month",
              "Date.Date",
              "Store.store.type",
              "inv.warehouseSales",
              "inv.warehouseCost")
        })
      }

      "querying a schema with filter" should {
        "add filter to a query having no filter defined" in {

          val dataSources = DsProviders.all

          val query = AggregateQuery(rows = List("Customer.firstName", "Customer.lastName"))

          val schema =
            """schema Foodmart (dataSource = "foodmart") {
              | filter "Customer.firstName in ('Jack', 'Jill') and Customer.firstName > 'A'"
              |  dimension Customer(table = "customer") {
              |    attribute firstName(column = "fname")
              |    attribute lastName(column = "lname")
              |  }
              |
              |  table customer {
              |    column fname
              |    column lname
              |  }
              |}
              |""".stripMargin

          test(schema, dataSources, query) { (statement, _) =>
            statement.logicalPlan mustBe
              """LogicalProject(Customer.firstName=[$0], Customer.lastName=[$1])
                |  LogicalAggregate(group=[{0, 1}])
                |    LogicalProject(Customer.firstName=[$3], Customer.lastName=[$2])
                |      LogicalFilter(condition=[AND(OR(=($3, 'Jack'), =($3, 'Jill')), >($3, 'A'))])
                |        JdbcTableScan(table=[[foodmart, customer]])
                |""".stripMargin
          }
        }
        "combine filter with the one contained in query" in {

          val dataSources = DsProviders.all

          val query =
            AggregateQuery(rows = List("Customer.firstName", "Customer.lastName"),
                           filter = Some(ref("Customer.firstName") > lit("A")))

          val schema =
            """schema Foodmart (dataSource = "foodmart") {
              |
              |  dimension Customer(table = "customer") {
              |    attribute firstName(column = "fname")
              |    attribute lastName(column = "lname")
              |  }
              |  filter "Customer.firstName in ('Jack', 'Jill')"
              |
              |  table customer {
              |    column fname
              |    column lname
              |  }
              |}
              |""".stripMargin

          test(schema, dataSources, query) { (statement, _) =>
            statement.logicalPlan mustBe
              """LogicalProject(Customer.firstName=[$0], Customer.lastName=[$1])
                |  LogicalAggregate(group=[{0, 1}])
                |    LogicalProject(Customer.firstName=[$3], Customer.lastName=[$2])
                |      LogicalFilter(condition=[AND(>($3, 'A'), OR(=($3, 'Jack'), =($3, 'Jill')))])
                |        JdbcTableScan(table=[[foodmart, customer]])
                |""".stripMargin
          }
        }
      }
    }
  }
}
