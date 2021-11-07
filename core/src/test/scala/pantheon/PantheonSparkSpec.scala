package pantheon

import java.sql.Date

import pantheon.util._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{MustMatchers, WordSpec}
import pantheon.backend.spark.SparkBackend
import pantheon.planner.ASTBuilders._
import pantheon.planner.PlannerException
import pantheon.schema.DsProviders
import pantheon.schema.Compatibility.getFields
import pantheon.schema._
import pantheon.util.RowSetUtil

import scala.language.implicitConversions

class PantheonSparkSpec extends WordSpec with MustMatchers with StrictLogging {

  implicit val ctx = Logging.newContext

  class Result(statement: Statement) {

    statement match {
      case s: StatementWithPlan => logger.debug(s"Pantheon plan: ${s.plan}")
      case _                    =>
    }

    logger.debug(s"Calcite logical plan: ${statement.backendLogicalPlan}")
    logger.debug(s"Calcite physical plan: ${statement.backendPhysicalPlan}")

    def logicalPlan: String = statement.backendLogicalPlan.toString

    def physicalPlan: String = statement.backendPhysicalPlan.toString

    def list(limit: Int = 100): List[String] = RowSetUtil.toStringList(statement.execute(), limit)

    def value(limit: Int = 100): String = RowSetUtil.toString(statement.execute(), limit)
  }

  val fixture = new SparkFoodmartSchemaFixture {}
  val catalog = new InMemoryCatalog(
    Map(
      "Sales" -> fixture.salesSchema,
      "Inventory" -> fixture.invSchema,
      "Foodmart" -> fixture.rootSchema
    ),
    List(DsProviders.hsqldbFoodmart.dsOpt.get))

  "Pantheon" when {
    def test(schemaPsl: String, query: PantheonQuery, deps: Map[String, String] = Map.empty)(
        block: Result => Unit): Unit = {
      val schemas = deps + ("_test_" -> schemaPsl)

      Pantheon.withConnection(new InMemoryCatalog(schemas, List(DsProviders.hsqldbFoodmart.dsOpt.get)),
                              "_test_",
                              SparkBackend()) { connection =>
        val statement = connection.createStatement(query)
        val result = new Result(statement)
        block(result)
      }
    }

    def toEntity(query: PantheonQuery): RecordQuery = query match {
      case q: AggregateQuery if q.pivoted => throw new Exception("cannot create Record query from Pivoted query")
      case q: AggregateQuery =>
        if (q.measures.nonEmpty)
          throw new Exception("cannot create Record query from AggregateQuery when measures is non empty")
        else RecordQuery(q.rows, q.filter, q.offset, q.limit, q.orderBy)
      case q: RecordQuery => q
    }

    def testCombined(schema: String, query: PantheonQuery)(block: (Result, Result) => Unit): Unit = {
      Pantheon.withConnection(new InMemoryCatalog(Map("_test_" -> schema), List(DsProviders.hsqldbFoodmart.dsOpt.get)),
                              "_test_",
                              SparkBackend()) { connection =>
        val statement = connection.createStatement(query)
        val entityStatement = connection.createStatement(toEntity(query))
        val result = new Result(statement)
        val entityResult = new Result(entityStatement)
        block(result, entityResult)
      }
    }

    def testWithFoodmart(query: PantheonQuery)(block: Result => Unit): Unit = {
      Pantheon.withConnection(catalog, "Foodmart", SparkBackend()) { connection =>
        val statement = connection.createStatement(query)
        val result = new Result(statement)
        block(result)
      }
    }

    def testWithFoodmartCombined(query: PantheonQuery)(block: (Result, Result) => Unit): Unit = {
      Pantheon.withConnection(catalog, "Foodmart", SparkBackend()) { connection =>
        val statement = connection.createStatement(query)
        val entityStatement = connection.createStatement(toEntity(query))

        val result = new Result(statement)
        val entityResult = new Result(entityStatement)
        block(result, entityResult)
      }
    }

    "querying a single table" should {
      "two attributes" in {
        val query = AggregateQuery(rows = List("Customer.firstName", "Customer.lastName"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute firstName(column = "fname")
            |    attribute lastName(column = "lname")
            |  }
            |  table customer {
            |    column fname
            |    column lname
            |  }
            |}
            |""".stripMargin

        testCombined(schema, query) { (result, entityResult) =>
          result.value(5) mustBe
            """Customer.firstName=Gary; Customer.lastName=Dumin
              |Customer.firstName=Angela; Customer.lastName=Bowers
              |Customer.firstName=Richard; Customer.lastName=Runyon
              |Customer.firstName=Shane; Customer.lastName=Belli
              |Customer.firstName=Sue; Customer.lastName=Hofsetz""".stripMargin

          entityResult.value(5) mustBe
            """Customer.firstName=Sheri; Customer.lastName=Nowmer
              |Customer.firstName=Derrick; Customer.lastName=Whelply
              |Customer.firstName=Jeanne; Customer.lastName=Derry
              |Customer.firstName=Michael; Customer.lastName=Spence
              |Customer.firstName=Maya; Customer.lastName=Gutierrez""".stripMargin
        }
      }

      "entity only query for dimension" in {
        val query = AggregateQuery(rows = List("Customer.id"))

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

        testCombined(schema, query) { (result, entityResult) =>
          result.value(5) mustBe
            """Customer.id=148
              |Customer.id=463
              |Customer.id=471
              |Customer.id=496
              |Customer.id=833""".stripMargin

          entityResult.value(5) mustBe
            """Customer.id=1
              |Customer.id=2
              |Customer.id=3
              |Customer.id=4
              |Customer.id=5""".stripMargin
        }
      }

      "null shall go last when sorting" in {

        val dataSources = DsProviders.all

        val query1 = AggregateQuery(rows = List("Customer.address2"),
          orderBy = List(OrderedColumn("Customer.address2")))

        val query2 = AggregateQuery(rows = List("Customer.address2"),
          orderBy = List(OrderedColumn("Customer.address2", SortOrder.Desc)))

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  dimension Customer(table = "customer") {
            |    attribute address2(column = "address2")
            |  }
            |}
            |""".stripMargin

        test(schema, query1) { result =>
          result.value(5) mustBe
            """Customer.address2= Apt. A
              |Customer.address2= Unit A
              |Customer.address2=#1
              |Customer.address2=#10
              |Customer.address2=#101""".stripMargin
        }
        test(schema, query2) { result =>
          result.value(5) mustBe
            """Customer.address2=Unit H103
              |Customer.address2=Unit H
              |Customer.address2=Unit G12
              |Customer.address2=Unit G 202
              |Customer.address2=Unit F13""".stripMargin
        }
      }

      "filtered measures singe table" in {
        val q3 = "\"\"\""

        val schema =
          s"""schema Foodmart (dataSource = "foodmart") {
             |  dimension X(table = "sales_fact_1998") {
             |    attribute tid(column = "time_id")
             |  }
             |
             |  measure unitSales( filter = "X.tid > 300", column = "sales_fact_1998.unit_sales")
             |
             | table sales_fact_1998 {
             |    column time_id
             |    column customer_id
             |    column unit_sales
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(measures = List("unitSales"))

        test(schema, query) { r =>
          r.value(1) mustBe "unitSales=509987.000"

        }
      }

      "filtered and calculated measures multiple tables" in {
        val q3 = "\"\"\""

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
             |  //Direct parernt child relation
             |  measure unitSales(column = "sales_fact_1998.unit_sales")
             |  measure unitSalesx(measure = "unitSales", filter =  "Customer.cars < 3")
             |
             |  measure numChildren(filter =  "Customer.cars < 3", column = "customer.num_children_at_home")
             |
             | //storeSales is not used in query directly of via filter
             |  measure calc1 (calculation = "storeSales + 100000 / (storeCost + storeSales) + 1")
             |  //checking division by zero
             |  measure calc2 (calculation = "storeSales + 100000 / ((-1 * storeCost) + storeCost)")
             |
             |
             |  //Indirect parent child relation
             |  measure storeCost(filter =  "X.tid > 1000", column = "sales_fact_1998.store_cost")
             |  measure storeSales(column = "sales_fact_1998.store_sales")
             |
             | table customer {
             |    column customer_id
             |    column num_children_at_home
             |    column num_cars_owned
             | }
             | table sales_fact_1998 {
             |    column time_id
             |    column customer_id(tableRef = "customer.customer_id")
             |    column unit_sales
             |    column store_sales
             |    column store_cost
             |  }
             |}
             |""".stripMargin

        val query = AggregateQuery(
          rows = List("Customer.id"),
          measures = List("calc1", "calc2", "storeCost", "unitSales", "unitSalesx", "numChildren"),
          orderBy = List(OrderedColumn("Customer.id")))

        test(schema, query) { r =>
          r.value(5) mustBe """Customer.id=1001; calc1=7506.163; calc2=null; storeCost=3.532; unitSales=3.000; unitSalesx=3.000; numChildren=1
                              |Customer.id=1002; calc1=638.304; calc2=null; storeCost=34.213; unitSales=186.000; unitSalesx=186.000; numChildren=2
                              |Customer.id=1003; calc1=610.111; calc2=null; storeCost=31.852; unitSales=151.000; unitSalesx=null; numChildren=null
                              |Customer.id=1004; calc1=639.296; calc2=null; storeCost=48.026; unitSales=200.000; unitSalesx=null; numChildren=null
                              |Customer.id=1005; calc1=629.966; calc2=null; storeCost=11.999; unitSales=165.000; unitSalesx=165.000; numChildren=0""".stripMargin

        }
      }

      "query with many possible permutations of parameters" in {

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  dimension Customer(table = "customer") {
            |    attribute id(columns = ["customer_id", "sales_fact_1998.customer_id"])
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |
            |  table sales_fact_1998 {
            |    column customer_id
            |    column unit_sales
            |  }
            |}
            |""".stripMargin

        val queries: List[AggregateQuery] = {

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
            offset <- List(0)
            limit <- List(None, Some(0), Some(1))
            ocs <- genOrderColumns(rows ++ mes)
              .groupBy(_.order)
              .values
              .toList
          } yield
            AggregateQuery(rows = rows, measures = mes, filter = filter, offset = offset, limit = limit, orderBy = ocs)
              .asInstanceOf[AggregateQuery]
        }

        //checking generation
        queries.size mustBe 36
        queries.size mustBe queries.distinct.size

        def validateResponse(q: AggregateQuery, result: Result): Option[String] = {

          val inspectionSize = 3

          val response = result.list(inspectionSize)

          def respEqualsChk(data: List[String]) = (response != data).option(
            s"RESPONSE:\n${response.mkString("\n")}\nWAS NOT EQUAL TO:\n${data.mkString("\n")}\n"
          )

          val orderOpt = {
            Predef.assert(
              q.orderBy.map(_.order).distinct.size <= 1,
              "Both orders are expected to be either asc or desc, change this logic if data has changed"
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
          test(schema, q) { result =>
            val errors = validateResponse(q, result)
            val success = errors.isEmpty
            assert(
              success,
              s"\nERRORS:\n${errors.mkString("\n")}\nWHILE TESTING QUERY:'$q\n"
            )
        })

      }

      "two dimensions with like" in {
        val query = AggregateQuery(rows = List("Customer.lastName"),
                              filter = Some(ref("Customer.lastName").like(lit("Cai%"), lit("%ley"))))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute lastName(column = "lname")
            |  }
            |  table customer {
            |    column lname
            |  }
            |}
            |""".stripMargin

        testCombined(schema, query) { (result, entityResult) =>
          result.value(7) mustBe
            """Customer.lastName=Bowley
              |Customer.lastName=Smedley
              |Customer.lastName=Handley
              |Customer.lastName=Maley
              |Customer.lastName=Wardley
              |Customer.lastName=Pelley
              |Customer.lastName=Farley""".stripMargin

          entityResult.value(7) mustBe
            """Customer.lastName=Caijem
              |Customer.lastName=Cain
              |Customer.lastName=Caise
              |Customer.lastName=Caiyam
              |Customer.lastName=Alley
              |Customer.lastName=Ashley
              |Customer.lastName=Bagley""".stripMargin
        }
      }

      "dimensions with expression" in {
        val query =
          AggregateQuery(rows = List("Customer.lastName", "Customer.birthDate", "Customer.birthYear", "Customer.birthMonth"))

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
            |    column birthYear(expression = "trunc(birthdate, 'YY')")
            |    column birthMonth(expression = "trunc(birthdate, 'MM')")
            |  }
            |}
            |""".stripMargin

        testCombined(schema, query) { (result, entityResult) =>
          result.value(7) mustBe
            """Customer.lastName=Sanchez; Customer.birthDate=1966-09-07; Customer.birthYear=1966-01-01; Customer.birthMonth=1966-09-01
                |Customer.lastName=Avalos; Customer.birthDate=1955-07-13; Customer.birthYear=1955-01-01; Customer.birthMonth=1955-07-01
                |Customer.lastName=Tedford; Customer.birthDate=1916-04-25; Customer.birthYear=1916-01-01; Customer.birthMonth=1916-04-01
                |Customer.lastName=Carlisle; Customer.birthDate=1919-05-07; Customer.birthYear=1919-01-01; Customer.birthMonth=1919-05-01
                |Customer.lastName=Hamrick; Customer.birthDate=1925-11-08; Customer.birthYear=1925-01-01; Customer.birthMonth=1925-11-01
                |Customer.lastName=Tallmadge; Customer.birthDate=1958-12-01; Customer.birthYear=1958-01-01; Customer.birthMonth=1958-12-01
                |Customer.lastName=Dunlap; Customer.birthDate=1927-07-17; Customer.birthYear=1927-01-01; Customer.birthMonth=1927-07-01""".stripMargin

          entityResult.value(7) mustBe
            """Customer.lastName=Nowmer; Customer.birthDate=1961-08-26; Customer.birthYear=1961-01-01; Customer.birthMonth=1961-08-01
                |Customer.lastName=Whelply; Customer.birthDate=1915-07-03; Customer.birthYear=1915-01-01; Customer.birthMonth=1915-07-01
                |Customer.lastName=Derry; Customer.birthDate=1910-06-21; Customer.birthYear=1910-01-01; Customer.birthMonth=1910-06-01
                |Customer.lastName=Spence; Customer.birthDate=1969-06-20; Customer.birthYear=1969-01-01; Customer.birthMonth=1969-06-01
                |Customer.lastName=Gutierrez; Customer.birthDate=1951-05-10; Customer.birthYear=1951-01-01; Customer.birthMonth=1951-05-01
                |Customer.lastName=Damstra; Customer.birthDate=1942-10-08; Customer.birthYear=1942-01-01; Customer.birthMonth=1942-10-01
                |Customer.lastName=Kanagaki; Customer.birthDate=1949-03-27; Customer.birthYear=1949-01-01; Customer.birthMonth=1949-03-01""".stripMargin
        }
      }

      "dimensions with expression and filter" in {
        val query = AggregateQuery(
          rows = List("Customer.day", "Customer.lastName"),
          measures = List("unitSales"),
          filter = Some(
            ref("Customer.day") >= lit(Date.valueOf("1950-01-01")) & ref("Customer.day") <= lit(
              Date.valueOf("2013-01-01")))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart", strict = false) {
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure linesCount (aggregate = "count", column = "sales_fact_1998.time_id")
            |
            |  dimension Customer (table = "customer") {
            |    attribute customer_id (column ="customer_id")
            |    attribute lastName (column = "lname")
            |    attribute birthDate (column = "birthdate")
            |    attribute year (column = "birthYear")
            |    attribute month (column = "birthMonth")
            |    attribute day (column = "birthdate")
            |  }
            |
            |  table sales_fact_1998 {
            |    column customer_id (tableRef = "customer.customer_id")
            |    column time_id
            |    column unit_sales
            |  }
            |  table customer {
            |    column birthYear(expression = "trunc(birthdate, 'YY')")
            |    column birthMonth(expression = "trunc(birthdate, 'MM')")
            |  }
            |}
          """.stripMargin

        test(schema, query) { result =>
          result.value(7) mustBe
            """Customer.day=1970-08-22; Customer.lastName=Dooley; unitSales=71.000
              |Customer.day=1963-03-06; Customer.lastName=Nightingale; unitSales=19.000
              |Customer.day=1975-12-19; Customer.lastName=Ellis; unitSales=10.000
              |Customer.day=1979-03-05; Customer.lastName=Espinoza; unitSales=11.000
              |Customer.day=1963-06-14; Customer.lastName=Creager; unitSales=7.000
              |Customer.day=1964-03-14; Customer.lastName=Wooldridge; unitSales=28.000
              |Customer.day=1958-11-06; Customer.lastName=Styles; unitSales=17.000""".stripMargin
        }
      }

      "single measure" in {
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

        test(schema, query) { result =>
          result.value() mustBe
            """unitSales=509987.000""".stripMargin
        }
      }

      "multiple measures" in {
        val query = AggregateQuery(measures = List("unitSales", "linesCount"))

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

        test(schema, query) { result =>
          result.value() mustBe
            """unitSales=509987.000; linesCount=164558""".stripMargin
        }
      }

      "single measure, single dimension" in {
        val query = AggregateQuery(rows = List("Store.id"), measures = List("unitSales"))

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

        test(schema, query) { result =>
          result.value(5) mustBe
            """Store.id=12; unitSales=37680.000
              |Store.id=22; unitSales=2244.000
              |Store.id=1; unitSales=23226.000
              |Store.id=13; unitSales=35346.000
              |Store.id=6; unitSales=22707.000""".stripMargin
        }
      }

      "single measure, multiple dimensions" in {
        val query = AggregateQuery(rows = List("Product.id", "Store.id"), measures = List("unitSales"))
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

        test(schema, query) { result =>
          result.value(5) mustBe
            """Product.id=1384; Store.id=1; unitSales=10.000
              |Product.id=307; Store.id=1; unitSales=13.000
              |Product.id=1504; Store.id=1; unitSales=16.000
              |Product.id=1161; Store.id=1; unitSales=12.000
              |Product.id=1530; Store.id=2; unitSales=3.000""".stripMargin
        }
      }

      "multiple measures, multiple dimensions" in {
        val query = AggregateQuery(rows = List("Product.id", "Store.id"), measures = List("unitSales", "linesCount"))

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

        test(schema, query) { result =>
          result.value(5) mustBe
            """Product.id=1384; Store.id=1; unitSales=10.000; linesCount=3
              |Product.id=307; Store.id=1; unitSales=13.000; linesCount=4
              |Product.id=1504; Store.id=1; unitSales=16.000; linesCount=5
              |Product.id=1161; Store.id=1; unitSales=12.000; linesCount=4
              |Product.id=1530; Store.id=2; unitSales=3.000; linesCount=2""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a queried dimension" in {
        val query = AggregateQuery(
          rows = List("Product.id", "Store.id"),
          measures = List("unitSales", "linesCount"),
          filter = Some(ref("Product.id") === lit(10))
        )

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Store {
            |    attribute id(column = "sales_fact_1998.store_id")
            |  }
            |  dimension Product {
            |    attribute id(column = "sales_fact_1998.product_id")
            |  }
            |  dimension UnitSales {
            |    attribute figure(column = "sales_fact_1998.unit_sales")
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

        test(schema, query) { result =>
          result.value(5) mustBe
            """Product.id=10; Store.id=2; unitSales=2.000; linesCount=2
              |Product.id=10; Store.id=15; unitSales=8.000; linesCount=2
              |Product.id=10; Store.id=24; unitSales=5.000; linesCount=1
              |Product.id=10; Store.id=20; unitSales=5.000; linesCount=2
              |Product.id=10; Store.id=13; unitSales=16.000; linesCount=6""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a non-queried dimension" in {
        val query = AggregateQuery(
          rows = List("Product.id", "Store.id"),
          measures = List("unitSales", "linesCount"),
          filter = Some(ref("Promotion.id").in(lit(54), lit(58)))
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

        test(schema, query) { result =>
          result.value(5) mustBe
            """Product.id=307; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=1187; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=108; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=1119; Store.id=1; unitSales=3.000; linesCount=1
              |Product.id=17; Store.id=1; unitSales=5.000; linesCount=2""".stripMargin
        }
      }

      "single measure, multiple dimensions, filter on a multiple queried dimensions" in {
        val query =
          AggregateQuery(
            rows = List("Product.id", "Store.id"),
            measures = List("unitSales", "linesCount"),
            filter = Some(
              ref("Product.id").notIn(Seq(10, 11, 12, 13, 14).map(lit(_)): _*) &
                ref("Store.id") >= lit(10) &
                ref("Store.id") <= lit(15)
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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Product.id=989; Store.id=10; unitSales=20.000; linesCount=6
              |Product.id=1226; Store.id=10; unitSales=19.000; linesCount=6
              |Product.id=662; Store.id=10; unitSales=19.000; linesCount=7
              |Product.id=1435; Store.id=10; unitSales=27.000; linesCount=8
              |Product.id=79; Store.id=10; unitSales=20.000; linesCount=6
              |Product.id=1115; Store.id=10; unitSales=18.000; linesCount=6""".stripMargin
        }
      }

      "pre and post aggregation filters combined" in {

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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Product.id=1; unitSalesFiltered=27.000
              |Product.id=2; unitSalesFiltered=54.000
              |Product.id=3; unitSalesFiltered=76.000
              |Product.id=4; unitSalesFiltered=43.000
              |Product.id=5; unitSalesFiltered=60.000
              |Product.id=6; unitSalesFiltered=72.000""".stripMargin
        }
      }

      "single measure, single star dimension" in {
        val query = AggregateQuery(rows = List("Customer.lastName"), measures = List("unitSales"))

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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Customer.lastName=Saiz; unitSales=97.000
              |Customer.lastName=Vissers; unitSales=357.000
              |Customer.lastName=Saraj; unitSales=79.000
              |Customer.lastName=Maddox; unitSales=39.000
              |Customer.lastName=Zastpil; unitSales=33.000
              |Customer.lastName=Palermo; unitSales=99.000""".stripMargin
        }
      }

      "single measure, single datetime dimension" in {
        val query =
          AggregateQuery(rows = List("Date.date"),
                    measures = List("unitSales"),
                    orderBy = List(OrderedColumn("unitSales", SortOrder.Desc)))

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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Date.date=1998-07-19 00:00:00; unitSales=4196.000
              |Date.date=1998-01-10 00:00:00; unitSales=3981.000
              |Date.date=1998-09-22 00:00:00; unitSales=3947.000
              |Date.date=1998-01-17 00:00:00; unitSales=3830.000
              |Date.date=1998-05-07 00:00:00; unitSales=3803.000
              |Date.date=1998-09-11 00:00:00; unitSales=3744.000""".stripMargin
        }
      }
    }

    "querying across multiple tables" should {
      "single measures, no dimensions" in {
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

        test(schema, query) { result =>
          result.value() mustBe
            """unitSales=509987.000; warehouseSales=348484.720""".stripMargin
        }
      }

      "single measures, degenerate dimension" in {
        val query = AggregateQuery(rows = List("Product.id"), measures = List("unitSales", "warehouseSales"))

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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Product.id=148; unitSales=322.000; warehouseSales=320.345
              |Product.id=463; unitSales=392.000; warehouseSales=114.563
              |Product.id=471; unitSales=363.000; warehouseSales=83.033
              |Product.id=496; unitSales=300.000; warehouseSales=107.957
              |Product.id=833; unitSales=348.000; warehouseSales=211.730
              |Product.id=1088; unitSales=276.000; warehouseSales=332.133""".stripMargin
        }
      }

      "single dimension, measures from different fact tables" in {
        val query = AggregateQuery(rows = List("Product.name"), measures = List("unitSales", "warehouseSales"))

        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Product(table = "product") {
            |    attribute id(columns = ["product_id", "sales_fact_1998.product_id", "inventory_fact_1998.product_id"])
            |    attribute name(column = "product_name")
            |  }
            |
            |  measure unitSales(column = "sales_fact_1998.unit_sales")
            |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
            |
            |  table product {
            |    column product_id
            |    column product_name
            |  }
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

        test(schema, query) { result =>
          result.value(6) mustBe
            """Product.name=Atomic Spicy Mints; unitSales=267.000; warehouseSales=115.245
              |Product.name=Ebony Mixed Nuts; unitSales=419.000; warehouseSales=61.057
              |Product.name=Golden Pancake Mix; unitSales=356.000; warehouseSales=128.124
              |Product.name=James Bay City Map; unitSales=338.000; warehouseSales=140.748
              |Product.name=Red Wing Scissors; unitSales=260.000; warehouseSales=136.169
              |Product.name=Steady Toothpaste; unitSales=301.000; warehouseSales=338.890""".stripMargin
        }
      }
    }

    "querying with exported schemas" should {
      "dimension from exported schema" in {
        val query = AggregateQuery(rows = List("sales.Store.region.city"))

        testWithFoodmartCombined(query) { (result, entityResult) =>
          result.value(6) mustBe
            """sales.Store.region.city=San Andres
              |sales.Store.region.city=Edmonds
              |sales.Store.region.city=Acapulco
              |sales.Store.region.city=Cliffside
              |sales.Store.region.city=Palo Alto
              |sales.Store.region.city=Everett""".stripMargin

          entityResult.value(6) mustBe
            """sales.Store.region.city=None
              |sales.Store.region.city=San Francisco
              |sales.Store.region.city=Mexico City
              |sales.Store.region.city=Los Angeles
              |sales.Store.region.city=Guadalajara
              |sales.Store.region.city=Vancouver""".stripMargin
        }
      }

      "measure and dimension from exported schema" in {
        val query = AggregateQuery(rows = List("sales.Store.region.city"), measures = List("sales.sales"))

        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """sales.Store.region.city=San Andres; sales.sales=72383.610
              |sales.Store.region.city=Acapulco; sales.sales=49090.030
              |sales.Store.region.city=Los Angeles; sales.sales=50819.150
              |sales.Store.region.city=Bellingham; sales.sales=4164.810
              |sales.Store.region.city=Hidalgo; sales.sales=100095.580
              |sales.Store.region.city=Victoria; sales.sales=20114.290""".stripMargin
        }
      }

      "conforming dimension and measure from exported schema" in {
        val query = AggregateQuery(rows = List("Store.region.city"), measures = List("sales.sales"))

        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=San Andres; sales.sales=72383.610
              |Store.region.city=Acapulco; sales.sales=49090.030
              |Store.region.city=Los Angeles; sales.sales=50819.150
              |Store.region.city=Bellingham; sales.sales=4164.810
              |Store.region.city=Hidalgo; sales.sales=100095.580
              |Store.region.city=Victoria; sales.sales=20114.290""".stripMargin
        }
      }

      "conforming dimension, dimension and measure from exported schema" in {
        val query = AggregateQuery(rows = List("Store.region.city", "sales.Customer.customer.lastName"),
                              measures = List("sales.sales"))

        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=Hidalgo; sales.Customer.customer.lastName=Storey; sales.sales=1142.290
              |Store.region.city=Hidalgo; sales.Customer.customer.lastName=Dvorak; sales.sales=1353.800
              |Store.region.city=Acapulco; sales.Customer.customer.lastName=Walker; sales.sales=347.000
              |Store.region.city=Beverly Hills; sales.Customer.customer.lastName=Glavaris; sales.sales=32.240
              |Store.region.city=Beverly Hills; sales.Customer.customer.lastName=Todoroff; sales.sales=14.730
              |Store.region.city=Beverly Hills; sales.Customer.customer.lastName=Retinski; sales.sales=113.710""".stripMargin

        }
      }

      "conforming dimension, measures from different exported schemas" in {
        val query = AggregateQuery(rows = List("Store.region.city"), measures = List("sales.sales", "inv.warehouseSales"))
        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=San Andres; sales.sales=72383.610; inv.warehouseSales=22291.583
              |Store.region.city=Acapulco; sales.sales=49090.030; inv.warehouseSales=23817.119
              |Store.region.city=Los Angeles; sales.sales=50819.150; inv.warehouseSales=23998.142
              |Store.region.city=Bellingham; sales.sales=4164.810; inv.warehouseSales=2026.280
              |Store.region.city=Hidalgo; sales.sales=100095.580; inv.warehouseSales=14279.903
              |Store.region.city=Victoria; sales.sales=20114.290; inv.warehouseSales=9447.087""".stripMargin
        }
      }

      "a couple of conforming dimension, measures from different exported schemas" in {
        val query =
          AggregateQuery(rows = List("Store.region.city", "Date.Month"),
                    measures = List("sales.sales", "inv.warehouseSales"))
        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=Victoria; Date.Month=October; sales.sales=1864.900; inv.warehouseSales=null
              |Store.region.city=Hidalgo; Date.Month=November; sales.sales=9345.330; inv.warehouseSales=2422.246
              |Store.region.city=San Andres; Date.Month=October; sales.sales=6328.110; inv.warehouseSales=2054.974
              |Store.region.city=Los Angeles; Date.Month=January; sales.sales=5143.210; inv.warehouseSales=3108.729
              |Store.region.city=Portland; Date.Month=September; sales.sales=5170.370; inv.warehouseSales=2119.042
              |Store.region.city=Spokane; Date.Month=February; sales.sales=5200.810; inv.warehouseSales=null""".stripMargin

        }
      }

      "a couple of conforming dimension, measures from different exported schemas with filtering" in {
        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales.sales", "inv.warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )
        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=Victoria; Date.Month=October; sales.sales=1864.900; inv.warehouseSales=null
              |Store.region.city=Hidalgo; Date.Month=November; sales.sales=9345.330; inv.warehouseSales=2422.246
              |Store.region.city=San Andres; Date.Month=October; sales.sales=6328.110; inv.warehouseSales=2054.974
              |Store.region.city=Los Angeles; Date.Month=January; sales.sales=5143.210; inv.warehouseSales=3108.729
              |Store.region.city=Portland; Date.Month=September; sales.sales=5170.370; inv.warehouseSales=2119.042
              |Store.region.city=Spokane; Date.Month=February; sales.sales=5200.810; inv.warehouseSales=null""".stripMargin
        }
      }

      "a couple of conforming dimension, conforming measures from different schemas with filtering" in {
        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )
        testWithFoodmart(query) { result =>
          result.value(6) mustBe
            """Store.region.city=Victoria; Date.Month=October; sales=1864.900; warehouseSales=null
              |Store.region.city=Hidalgo; Date.Month=November; sales=9345.330; warehouseSales=2422.246
              |Store.region.city=San Andres; Date.Month=October; sales=6328.110; warehouseSales=2054.974
              |Store.region.city=Los Angeles; Date.Month=January; sales=5143.210; warehouseSales=3108.729
              |Store.region.city=Portland; Date.Month=September; sales=5170.370; warehouseSales=2119.042
              |Store.region.city=Spokane; Date.Month=February; sales=5200.810; warehouseSales=null""".stripMargin
        }
      }

      "incompatible dimensions from multiple exported schemas" in {
        val query = AggregateQuery(rows = List("sales.Date.Month", "inv.Date.Month"))
        assertThrows[PlannerException] {
          testWithFoodmart(query) { result =>
            result.logicalPlan
          }
        }

      }

      "incompatible dimension and measure from different exported schemas" in {
        val query = AggregateQuery(rows = List("sales.Date.Month"), measures = List("inv.warehouseSales"))
        assertThrows[PlannerException] {
          testWithFoodmart(query) { result =>
            result.logicalPlan
          }
        }
      }

      "support direct SQL queries" in {

        Pantheon.withConnection(catalog, "Foodmart", SparkBackend()) { connection =>
          val statement = connection.createStatement(SqlQuery("""select lname from `sales.customer`"""))
          val rs = statement.execute()
          RowSetUtil.toString(rs, 5) mustBe
            """lname=Nowmer
                |lname=Whelply
                |lname=Derry
                |lname=Spence
                |lname=Gutierrez""".stripMargin

          rs.close()
        }
      }
    }

    "querying a view on table" should {
      val q3 = "\"\"\""

      "dimensions only" in {
        val query = AggregateQuery(rows = List("Customer.id", "Customer.year", "Customer.day", "Customer.month"))

        val schema =
          // TODO: dataSource may be omitted (fix this test after separating RawTable and CompiledTable)
          s"""schema Foodmart(dataSource = "foodmart") {
             | // \\n is supported in tripplequotes
             | table viewTable(sql = $q3
             |   select sf8.*, td."the_year", td."the_month", td."day_of_month", td."the_date"
             |   from "sales_fact_1998" sf8
             |   join "time_by_day" td
             |   on (sf8."time_id" = td."time_id")
             | $q3) {
             |    column customer_id
             |    column the_year
             |    column the_month
             |    column day_of_month
             |    //Spark throws ParseException, fix it!
             |    //column day_of_week(expression = $q3 extract(day from "the_date")$q3)
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

        testCombined(schema, query) { (result, entityResult) =>
          result.value(6) mustBe """Customer.id=7929; Customer.year=1998; Customer.day=7; Customer.month=February
                                   |Customer.id=9586; Customer.year=1998; Customer.day=19; Customer.month=March
                                   |Customer.id=1948; Customer.year=1998; Customer.day=9; Customer.month=May
                                   |Customer.id=5065; Customer.year=1998; Customer.day=20; Customer.month=September
                                   |Customer.id=3032; Customer.year=1998; Customer.day=27; Customer.month=October
                                   |Customer.id=8177; Customer.year=1998; Customer.day=23; Customer.month=July""".stripMargin

          entityResult.value(6) mustBe """Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January
                                         |Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January
                                         |Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January
                                         |Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January
                                         |Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January
                                         |Customer.id=2094; Customer.year=1998; Customer.day=17; Customer.month=January""".stripMargin
        }

      }

      "dimensions and measures" in {
        val query = AggregateQuery(rows = List("Customer.month", "Customer.day"), measures = List("unitSales"))
        val schema =
          // TODO: dataSource may be omitted (fix this test after separating RawTable and CompiledTable)
          s"""schema Foodmart(dataSource = "foodmart")  {
             |
             |  measure unitSales(column = "viewTable.unit_sales")
             |
             | table viewTable(sql = $q3 select sf8.*, td."the_year", td."the_month", td."day_of_month" from "foodmart"."sales_fact_1998" sf8 join "foodmart"."time_by_day" td on (sf8."time_id" = td."time_id") $q3) {
             |    column customer_id
             |    column the_year
             |    column unit_sales
             |    column the_month
             |    column day_of_month
             |     // Spark throws ParseException, fix it!
             |    // column day_of_week(expression = $q3 extract(day from "the_date")$q3)
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

        test(schema, query) { (result) =>
          result.value(3) mustBe """Customer.month=March; Customer.day=14; unitSales=630.000
                                   |Customer.month=March; Customer.day=13; unitSales=374.000
                                   |Customer.month=May; Customer.day=18; unitSales=204.000""".stripMargin

        }
      }
    }

    "import/export" should {
      "work correctly with imported schemas" in {
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

        test(schema,
             AggregateQuery(rows = List("Store.region"), measures = List("sales", "warehouseSales")),
             Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)) { result =>
          result.value(6) mustBe
            """Store.region=78; sales=4164.810; warehouseSales=2026.280
              |Store.region=28; sales=49090.030; warehouseSales=23817.119
              |Store.region=76; sales=51135.190; warehouseSales=7260.913
              |Store.region=26; sales=79063.130; warehouseSales=7142.799
              |Store.region=27; sales=50047.510; warehouseSales=23140.649
              |Store.region=22; sales=53633.260; warehouseSales=25343.950""".stripMargin
        }

      }

      // Uncomment when import of fields is supported
//      "work correctly with imported measures/dimensions" in {
//        val schema =
//          """schema Foodmart {
//            |  import Sales.Store
//            |  import Inventory.{Store => InvStore}
//            |  import Sales.sales
//            |  import Inventory.{warehouseSales => invSales}
//            |
//            |  dimension ConformingStore {
//            |    conforms Store
//            |    conforms InvStore
//            |  }
//            |
//            |  measure totalSales {
//            |    conforms sales
//            |  }
//            |
//            |  measure warehouseSales {
//            |    conforms invSales
//            |  }
//            |}""".stripMargin
//
//        test(
//          schema,
//          AggregateQuery(rows = List("ConformingStore.region"), measures = List("totalSales", "warehouseSales")),
//          Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)
//        ) { result =>
//          result.value(6) mustBe
//            """ConformingStore.region=78; totalSales=4164.810; warehouseSales=2026.280
//              |ConformingStore.region=28; totalSales=49090.030; warehouseSales=23817.119
//              |ConformingStore.region=76; totalSales=51135.190; warehouseSales=7260.913
//              |ConformingStore.region=26; totalSales=79063.130; warehouseSales=7142.799
//              |ConformingStore.region=27; totalSales=50047.510; warehouseSales=23140.649
//              |ConformingStore.region=22; totalSales=53633.260; warehouseSales=25343.950""".stripMargin
//        }
//
//      }

      // Uncomment when import of fields is supported
//      "work correctly with exported measures/dimensions" in {
//        val schema =
//          """schema Foodmart {
//            |  export Sales.{Store, sales}
//            |  export Inventory.{Store => InvStore, warehouseSales => invSales}
//            |
//            |  dimension ConformingStore {
//            |    conforms Store
//            |    conforms InvStore
//            |  }
//            |}""".stripMargin
//
//        test(schema,
//             AggregateQuery(measures = List("invSales", "sales")),
//             Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)) { result =>
//          result.value(6) mustBe
//            """invSales=348484.720; sales=1079147.470""".stripMargin
//        }
//
//        test(schema,
//             AggregateQuery(rows = List("Store.region"), measures = List("sales")),
//             Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)) { result =>
//          result.value(6) mustBe
//            """Store.region=78; sales=4164.810
//              |Store.region=28; sales=49090.030
//              |Store.region=76; sales=51135.190
//              |Store.region=27; sales=50047.510
//              |Store.region=26; sales=79063.130
//              |Store.region=22; sales=53633.260""".stripMargin
//        }
//
//        test(
//          schema,
//          AggregateQuery(rows = List("ConformingStore.region"), measures = List("sales", "invSales")),
//          Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)
//        ) { result =>
//          result.value(6) mustBe
//            """ConformingStore.region=78; sales=4164.810; invSales=2026.280
//              |ConformingStore.region=28; sales=49090.030; invSales=23817.119
//              |ConformingStore.region=76; sales=51135.190; invSales=7260.913
//              |ConformingStore.region=26; sales=79063.130; invSales=7142.799
//              |ConformingStore.region=27; sales=50047.510; invSales=23140.649
//              |ConformingStore.region=22; sales=53633.260; invSales=25343.950""".stripMargin
//        }
//
//        test(schema,
//             AggregateQuery(rows = List("InvStore.region"), measures = List("invSales")),
//             Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)) { result =>
//          result.value(6) mustBe
//            """InvStore.region=78; invSales=2026.280
//              |InvStore.region=28; invSales=23817.119
//              |InvStore.region=76; invSales=7260.913
//              |InvStore.region=27; invSales=23140.649
//              |InvStore.region=26; invSales=7142.799
//              |InvStore.region=22; invSales=25343.950""".stripMargin
//        }
//
//        assertThrows[PlannerException] {
//          test(schema,
//               AggregateQuery(rows = List("InvStore.region"), measures = List("sales")),
//               Map("Inventory" -> fixture.invSchema, "Sales" -> fixture.salesSchema)) { result =>
//            result.logicalPlan
//          }
//        }
//      }
    }

    "compatibility query" should {
      "succeed" in {
        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )

        Pantheon.withConnection(catalog, "Foodmart", SparkBackend()) { connection =>
          getFields(connection.schema.getCompatibility(query)) mustBe Set(
            "Store.region",
            "Store.region.province",
            "Store.region.city",
            "Store.store.name",
            "Store.region.region",
            "Date.Year",
            "Date.Month",
            "Date.Date",
            "Store.region.country",
            "Store.store",
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
        val query = AggregateQuery(
          rows = List("Store.region.city", "Date.Month"),
          measures = List("sales", "warehouseSales"),
          filter = Some(ref("Date.Year").in(lit(1997), lit(1998)))
        )

        Pantheon.withConnection(catalog, "Foodmart", SparkBackend()) { connection =>
          getFields(connection.schema.getCompatibility(query, List("Store.store.", "Date.", "inv."))) mustBe Set(
            "Store.store.name",
            "Date.Year",
            "Date.Month",
            "Date.Date",
            "Store.store.type",
            "inv.warehouseSales",
            "inv.warehouseCost")
        }
      }

      "querying a schema with filter" should {
        "add filter to a query having no filter defined" in {
          val query = AggregateQuery(rows = List("Customer.firstName", "Customer.lastName"))

          val schema =
            """schema Foodmart (dataSource = "foodmart") {
              | filter "Customer.firstName in ('Jack', 'Jill') and Customer.firstName > 'A'"
              |  dimension Customer(table = "customer") {
              |    attribute firstName(column = "fname")
              |    attribute lastName(column = "lname")
              |  }
              |  table customer {
              |    column fname
              |    column lname
              |  }
              |}
              |""".stripMargin

          test(schema, query) { result =>
            result.value(6) mustBe
              """Customer.firstName=Jack; Customer.lastName=Baccus
                |Customer.firstName=Jack; Customer.lastName=Osteen
                |Customer.firstName=Jill; Customer.lastName=Christie
                |Customer.firstName=Jack; Customer.lastName=Parente
                |Customer.firstName=Jack; Customer.lastName=Gant
                |Customer.firstName=Jack; Customer.lastName=Thompson""".stripMargin
          }
        }

        "combine filter with the one contained in query" in {
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
              |  table customer {
              |    column fname
              |    column lname
              |  }
              |  filter "Customer.firstName in ('Jack', 'Jill')"
              |}
              |""".stripMargin

          test(schema, query) { result =>
            result.value(6) mustBe
              """Customer.firstName=Jack; Customer.lastName=Baccus
                |Customer.firstName=Jack; Customer.lastName=Osteen
                |Customer.firstName=Jill; Customer.lastName=Christie
                |Customer.firstName=Jack; Customer.lastName=Parente
                |Customer.firstName=Jack; Customer.lastName=Gant
                |Customer.firstName=Jack; Customer.lastName=Thompson""".stripMargin
          }
        }
      }
    }
  }
}
