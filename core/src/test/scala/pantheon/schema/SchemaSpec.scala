package pantheon.schema

import org.scalatest.{Matchers, WordSpec}
import pantheon.{InMemoryCatalog, AggregateQuery, ReflectiveDataSource}
import pantheon.schema.parser.SchemaParser
import pantheon.planner.ASTBuilders._
import pantheon.schema.Compatibility.getFields
import pantheon.planner.ASTBuilders._
import MeasureAggregate.Sum
import org.scalatest.prop.TableDrivenPropertyChecks

class SchemaSpec extends WordSpec with Matchers with TableDrivenPropertyChecks{
  def parse(schema: String): Either[List[SchemaCompilationError], SchemaAST] =
    SchemaParser(schema).left.map(e => List(SchemaCompilationError(e)))

  private val f = new FoodmartSchemaFixture {}

  val ds = DsProviders.defaultDs
  val foodmartSchema = new InMemoryCatalog(f.allSchemas, List(ds)).getSchema("Foodmart").get

  def compile(psl: String, additionalSchemas: (String, String)*): Schema =
    new InMemoryCatalog(Map("_test_" -> psl) ++ additionalSchemas.toMap, List(ds)).getSchema("_test_").get

  "Schema" when {
    "accessing dimensions" should {

      "return correct dimension for level" in {
        assert(foodmartSchema.getDimensionAttribute("Store.region").nonEmpty)
      }

      "return correct dimension for attribute inside level" in {
        assert(foodmartSchema.getDimensionAttribute("Store.region.city").nonEmpty)
      }

      "return correct dimension attribute for level in imported schema" in {
        assert(foodmartSchema.getDimensionAttribute("sales.Customer.customer").nonEmpty)
      }

      "return correct dimension attribute for attribute in imported schema" in {
        assert(foodmartSchema.getDimensionAttribute("sales.Customer.customer.firstName").nonEmpty)
      }

    }

    "specifying metadata" should {
      "parse metadata correctly" in {
        val psl = s"""schema Foodmart(dataSource = "${ds.name}", strict = false) {
            |  dimension D(table = "T") {
            |    metadata(d1 = ["d1", "dd1"], d2 = "d2")
            |    hierarchy H {
            |      metadata(h1 = ["h1", "hh1"], h2 = "h2")
            |      level L {
            |        metadata(l1 = ["l1", "ll1"], l2 = "l2", l3 = true)
            |        conforms l2
            |        attribute A {
            |          metadata(a1 = ["a1", "aa1"], a2 = "a2")
            |          conforms a2
            |        }
            |      }
            |    }
            |    hierarchy H2 {
            |      level L {
            |        metadata(l1 = ["l1", "ll1"], l2 = "l2")
            |      }
            |    }
            |  }
            |  measure M (column = "T.m") {
            |    metadata(m1 = ["m1", "mm1"], m2 = "m2")
            |  }
            |  table T {
            |   column m
            |  }
            |}
            |""".stripMargin
        val schema = compile(psl)

        val dimension = schema.getDimension("D").getOrElse(fail("Dimension D not found"))
        dimension.metadata.size shouldBe 2
        dimension.metadata("d1").v shouldBe List(StringValue("d1"), StringValue("dd1"))
        dimension.metadata("d2").v shouldBe "d2"

        val hierarchy = dimension.hierarchies.find(_.name == "H").getOrElse(fail("Hierarchy H not found"))
        hierarchy.metadata.size shouldBe 2
        hierarchy.metadata("h1").v shouldBe List(StringValue("h1"), StringValue("hh1"))
        hierarchy.metadata("h2").v shouldBe "h2"

        val level = hierarchy.levels.find(_.name == "L").getOrElse(fail("Level L not found"))
        level.metadata.size shouldBe 3
        level.metadata("l1").v shouldBe List(StringValue("l1"), StringValue("ll1"))
        level.metadata("l2").v shouldBe "l2"
        level.metadata("l3").v shouldBe true

        val attribute = level.attributes.find(_.name == "A").getOrElse(fail("Attribute A not found"))
        attribute.metadata.size shouldBe 2
        attribute.metadata("a1").v shouldBe List(StringValue("a1"), StringValue("aa1"))
        attribute.metadata("a2").v shouldBe "a2"

        val attribute2 = schema.getDimensionAttribute("D.H2.L").getOrElse(fail("Attribute D.H2.L not found"))
        attribute2.metadata.size shouldBe 2
        attribute2.metadata("l1").v shouldBe List(StringValue("l1"), StringValue("ll1"))
        attribute2.metadata("l2").v shouldBe "l2"

        val measure = schema.getMeasure("M").getOrElse(fail("Measure M not found"))
        measure.metadata.size shouldBe 2
        measure.metadata("m1").v shouldBe List(StringValue("m1"), StringValue("mm1"))
        measure.metadata("m2").v shouldBe "m2"
      }
    }

    "validating" should {
      "return zero errors for valid schema" in {
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
        compile(schema)
      }

      "return error for duplicate names" in {
        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute lastName(column = "lname")
            |  }
            |  measure Customer(column = "sales_fact_1998.unit_sales")
            |
            |  table sales_fact_1998 {
            |    column customer_id (tableRef = "customer.customer_id")
            |    column unit_sales
            |  }
            |}
            |""".stripMargin
        assertThrows[Exception](compile(schema))
      }

      "return errors for invalid references" in {
        val schema =
          """schema Foodmart (dataSource = "foodmart") {
            |  dimension Customer(table = "customer") {
            |    attribute id(column = "anothertable.customer_id")
            |    attribute lastName(column = "lname")
            |  }
            |  measure unitSales(column = "sales_fact_1998.unit_sales_error")
            |
            |  table sales_fact_1998 {
            |    column customer_id (tableRef = "errorTable.customer_id")
            |    column unit_sales
            |  }
            |
            |  table customer {
            |    column fname
            |  }
            |
            |  table sometable {
            |    column some_col
            |  }
            |}
            |""".stripMargin
        intercept[Exception](compile(schema)).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Invalid tableRef: 'TableRef(errorTable,customer_id,Left)' at table: 'sales_fact_1998', column: 'customer_id'," +
          "Attribute Customer.id referencing non-existent table column anothertable.customer_id," +
          "Attribute Customer.lastName referencing non-existent table column lname," +
          "Measure unitSales referencing non-existent table column sales_fact_1998.unit_sales_error," +
          "measure 'unitSales' is not referenced from any table," +
          "attribute 'Customer.id' is not referenced from any table," +
          "attribute 'Customer.lastName' is not referenced from any table"
      }
    }

    "importing" should {
      "import successfully" in {
        val gaPsl = s"""schema GoogleAnalytics(dataSource = "${ds.name}", strict = false) {
            |  dimension Time(table = "T") {
            |    attribute year
            |    attribute month
            |    attribute date
            |  }
            |
            |  dimension Geo(table = "T") {
            |    attribute continent
            |    attribute country
            |  }
            |  filter "Time.year > 1980"
            |
            |  measure sessions(column = "T.sessions")
            |  measure transactions(column = "T.transactions")
            |
            |  table T {
            |   column sessions
            |   column transactions
            |  }
            |}
          """.stripMargin

        val fbPsl = s"""schema FacebookStats(dataSource = "${ds.name}", strict = false) {
            |  dimension Time (table = "T"){
            |    attribute year
            |    attribute month
            |    attribute date
            |  }
            |
            |  dimension Channel(table = "T2") {
            |    attribute name
            |  }
            |
            |  filter "Channel.name = 'X'"
            |  measure sessions(column = "T2.sessions")
            |  measure cost(column = "T2.transactions")
            |
            |  table T2 {
            |   column sessions
            |   column transactions
            |  }
            |}
          """.stripMargin

        val foodmartPsl =
          s"""schema Foodmart(strict = false) {
            |  import {GoogleAnalytics => ga, FacebookStats => fb}
            |  dimension Customer(table = "ga.T") {
            |    attribute name
            |  }
            |
            |  measure amount(column = "T3.amount")
            |  measure count(column = "T3.count")
            |  measure totalValue(column = "T3.totalValue")
            |  table T3 (dataSource = "${ds.name}"){
            |   column amount
            |   column count
            |   column totalValue
            |  }
            |}
          """.stripMargin

        val combinedSchema = new InMemoryCatalog(Map(
                                                   "GoogleAnalytics" -> gaPsl,
                                                   "FacebookStats" -> fbPsl,
                                                   "Foodmart" -> foodmartPsl
                                                 ),
          List(ds)).getSchema("Foodmart").get

        combinedSchema.getDimensionAttribute("ga.Time.year").get.name shouldBe "year"
        combinedSchema.getMeasure("fb.sessions").get.name shouldBe "fb.sessions"
        combinedSchema.defaultFilter shouldBe None
      }
    }
    "Schema with imports and filtered measures" should {
      "have proper excludes and all requited refs imported and properly aliased" in {
        forAll(
          Table[String, String, Set[String]](
            ("import statement", "prefix", "excludes"),
            ("import GoogleAnalytics._", "", Set("sessions", "fact", "bars", "City", "transactions", "Store")),
            ("export GoogleAnalytics._", "", Set()),
            ("import {GoogleAnalytics => aga}", "aga." , Set("aga")),
            ("export {GoogleAnalytics => aga}", "aga." , Set()),
            ("import GoogleAnalytics", "GoogleAnalytics." , Set("GoogleAnalytics")),
            ("export GoogleAnalytics", "GoogleAnalytics." , Set()),
            ("import {GoogleAnalytics}", "GoogleAnalytics.", Set("GoogleAnalytics")),
            ("export {GoogleAnalytics}", "GoogleAnalytics.", Set())
          )
        ) {(importStmt, prefix, excludes) =>


          val gaPsl = """schema GoogleAnalytics(dataSource = "ds") {
                          |  measure sessions(filter="Store.Foo.Bar > 1", column = "fact.sessions")
                          |  measure transactions(column = "fact.transactions")
                          |  dimension Store{
                          |    hierarchy Foo{
                          |       level Bar(table = "bars") {
                          |         attribute sales
                          |      }
                          |    }
                          |  }
                          |  dimension City{
                          |     level Street(table = "bars") {
                          |       attribute house
                          |     }
                          |  }
                          |  table bars {
                          |    column L
                          |    column id
                          |    column house
                          |    column Street
                          |    column sales
                          |    column Bar
                          |  }
                          |  table fact {
                          |   column id (tableRef = "bars.id")
                          |   column sessions
                          |   column transactions
                          |  }
                          |  filter "City.Street = 'baz'"
                          |}
                        """.stripMargin

          val foodmartPsl =
            s"""schema Foodmart(dataSource = "ds") {
                |  $importStmt
                |  measure totalValue(filter="${prefix}Store.Foo.Bar > 1", column = "fact2.totalValue")
                |
                |  dimension D(table = "${prefix}bars"){
                |   level L
                |  }
                |  // TODO: measure name not containing dot cannot end with a number - fix filterparser
                |  measure amountx(column = "fact2.amountx")
                |  measure amounty(filter="D.L > 1", column = "fact2.amounty")
                |
                |  table fact2 {
                |   column id (tableRef = "${prefix}bars.id")
                |   column totalValue
                |   column amountx
                |   column amounty
                |  }
                |}
              """.stripMargin

          val reflDs = ReflectiveDataSource("ds", ())

          val combinedSchema = new InMemoryCatalog(
            Map(
              "GoogleAnalytics" -> gaPsl,
              "Foodmart" -> foodmartPsl
            ),
            List(reflDs)
          ).getSchema("Foodmart").get

          combinedSchema.name shouldBe "Foodmart"

          combinedSchema.dimensions should contain only (
            Dimension("D",Some(s"${prefix}bars"),Map(),List(),
              List(Hierarchy("",None,Map(),
                List(Level("L",None,Nil,None,None,Map(),List(),
                  List(Attribute("",None,Nil,None,None,Map(),List()))))))),
            Dimension(
              s"${prefix}City", None, Map(), List(),
              List(
                Hierarchy("", None, Map(),
                  List(Level("Street", Some(s"${prefix}bars"), Nil,None,None, Map(), List(),
                    List(Attribute("", Some(s"${prefix}bars"), Nil, None, None, Map(), List()),
                         Attribute("house", None, Nil, None, None, Map(), List()))
                  ))
                ))
            ),
            Dimension(
              s"${prefix}Store", None, Map(), List(),
              List(
                Hierarchy("Foo", None, Map(),
                  List(Level("Bar", Some(s"${prefix}bars"), Nil, None, None, Map(), List(),
                    List(Attribute("", Some(s"${prefix}bars"), Nil, None, None, Map(), List()),
                         Attribute("sales", None, Nil, None, None, Map(), List()))
                  ))
                ))
            )
          )

          combinedSchema.measures should contain only (
            FilteredMeasure(
              s"${prefix}sessions",
              AggMeasure(s"${prefix}sessions", Sum, List(s"${prefix}fact.sessions"), Map(), List()),
              ref(s"${prefix}Store.Foo.Bar") > lit(1),
              Map()
            ),
            AggMeasure(s"${prefix}transactions", Sum, List(s"${prefix}fact.transactions"), Map(), List()),
            FilteredMeasure("totalValue",
                            AggMeasure("totalValue", Sum, List("fact2.totalValue"), Map(), List()),
                            ref(s"${prefix}Store.Foo.Bar") > lit(1),
                            Map()),
            AggMeasure("amountx", Sum, List("fact2.amountx"), Map(), List()),
            FilteredMeasure("amounty", AggMeasure("amounty", Sum, List("fact2.amounty"), Map(), List()), ref("D.L") > lit(1), Map())
          )

          combinedSchema.tables
            .sortBy(_.name)
            .toString
            .replaceAll("\\s", "") shouldBe
              s"""List(
                 Table(${prefix}bars,
                   Left(PhysicalTableName(bars)),ReflectiveDataSource(ds,()),
                   List(Column(L,None,Set(D.L),Set(),Set()),
                     Column(id,None,Set(),Set(),Set()),
                     Column(house,None,Set(${prefix}City.Street.house),Set(),Set()),
                     Column(Street,None,Set(${prefix}City.Street),Set(),Set()),
                     Column(sales,None,Set(${prefix}Store.Foo.Bar.sales),Set(),Set()),
                     Column(Bar,None,Set(${prefix}Store.Foo.Bar),Set(),Set())),
                     false),
                 Table(${prefix}fact,
                   Left(PhysicalTableName(fact)),ReflectiveDataSource(ds,()),
                   List(
                     Column(id,None,Set(),Set(),Set(TableRef(${prefix}bars,id,Left))),
                     Column(sessions,None,Set(),Set(${prefix}sessions),Set()),
                     Column(transactions,None,Set(),Set(${prefix}transactions),Set())),
                     false),
                 Table(fact2,
                   Left(PhysicalTableName(fact2)),ReflectiveDataSource(ds,()),
                   List(
                     Column(id,None,Set(),Set(),Set(TableRef(${prefix}bars,id,Left))),
                     Column(totalValue,None,Set(),Set(totalValue),Set()),
                     Column(amountx,None,Set(),Set(amountx),Set()),
                     Column(amounty,None,Set(),Set(amounty),Set())),
                     false)
                     )""".replaceAll("\n", "").replaceAll("\\s", "")

          assert(combinedSchema.defaultFilter.isEmpty)
          combinedSchema.excludes shouldBe excludes
        }
      }
    }


    "importing/exporting" should {

      "fail with duplicate columns" in {
        val gaPsl =
          """schema DUP(strict = false, dataSource = "ds") {
            | table A{
            |  column a
            |  column b
            |  column a
            |  column c
            |  column b
            | }
            |}""".stripMargin

        intercept[Exception](
          new InMemoryCatalog(
            Map("DUP" -> gaPsl),
            List(ReflectiveDataSource("ds", ()))
          ).getSchema("DUP").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "table 'A' contains columns with duplicate names [a,b]"
      }


      "fail with duplicated table/measure/dimmension names" in {

        forAll(Table("importType", "import", "export")){ importType =>

        val gaPsl = """schema GoogleAnalytics(strict = false) {
            |  dimension X(table = "sales" ) {
            |    attribute date
            |  }
            |
            |  dimension Geo(table = "sales" ) {
            |    attribute country
            |  }
            |
            |  measure sessions(column = "sales.sessions")
            |  measure transactions(column = "sales.transactions")
            |
            |  table sales(dataSource = "ds") {
            |   column sessions
            |   column transactions
            |  }
            |
            |}
          """.stripMargin

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds", strict = false) {
            |  $importType GoogleAnalytics._
            |
            |  dimension Geo(table = "sales") {
            |    attribute city
            |  }
            |
            |  measure X(column = "sales.X")
            |  measure count(column = "sales.count")
            |  measure sessions(column = "sales.sessions")
            |
            |  table sales {
            |   column count
            |   column sessions
            |   column X
            |  }
            |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
                                "GoogleAnalytics" -> gaPsl,
                                "Foodmart" -> foodmartPsl
                              ),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Measure transactions referencing non-existent table column sales.transactions,Ambiguous dimension/measure name: 'Geo',Ambiguous dimension/measure name: 'X',Ambiguous dimension/measure name: 'sessions',Ambiguous table name: 'sales'"
        }
      }

      // allowing this may lead to confusions while redaing/using schemas, also schema structure will be cluttered with similar values(harder to debug in case of problems)
      "fail when same schema is imported/exported more than once" in {

        forAll(Table("importStmt",
          "import  GA._ \n  export GA",
          "import  GA._ \n  export GA._",
          "import  GA   \n  import GA",
          "import  GA._ \n  import GA._",
          "export  GA._ \n  export GA._",
          "export  GA   \n  export GA")){ importStmt =>

          val gaPsl = """schema GA {
                        |  measure sessions(column = "sales.sessions")
                        |  table sales(dataSource = "ds") {
                        |   column sessions
                        |  }
                        |
                        |}
                      """.stripMargin

          val foodmartPsl =
            s"""schema Foodmart(dataSource = "ds") {
               |   $importStmt

               |  measure count(column = "sales.count")
               |  table sales {
               |   column count
               |  }
               |}
          """.stripMargin

          intercept[Exception](
            new InMemoryCatalog(Map(
              "GA" -> gaPsl,
              "Foodmart" -> foodmartPsl
            ),
              List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
            "schemas [GA] are imported/exported more than once"
        }
      }

      // TODO add additional tests
//      "fail on broken measureRef and tableRef" in {
//
//        val gaPsl = """schema GA {
//                      |  measure sessions(column = "sales.sessions")
//                      |  table sales(dataSource = "ds") {
//                      |   column sessions
//                      |  }
//                      |
//                      |}
//                    """.stripMargin
//
//        val foodmartPsl =
//          s"""schema Foodmart(dataSource = "ds") {
//             |   import GA._
//             |
//             |  table sales2 {
//             |   column sessions(measureRef = "GA.sessions")
//             |   column X(measureRef = "X")
//             |  }
//             |}
//          """.stripMargin
//
//        intercept[Exception](
//          new InMemoryCatalog(Map(
//            "GA" -> gaPsl,
//            "Foodmart" -> foodmartPsl
//          ),
//            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
//          "invalid measureRef: 'GA.sessions' at table: 'sales2', column: 'sessions'," +
//          "invalid measureRef: 'X' at table: 'sales2', column: 'X'"
//      }

      "fail when filtered measure references another filtered measure or calculated measure in measure" in {

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds") {
             |
             |  measure calculated (calculation = "1 + 2")
             |  measure filtered(filter = "calculated > 1")
             |
             |  measure measureA(measure = "calculated", filter = "sessions > 1")
             |  measure measureB(measure = "filtered", filter = "calculated > 1")
             |
             |  table sales {
             |   column count
             |   column sessions
             |   column X
             |  }
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Reference 'calculated' is of incompatible type (only Aggregated measures are allowed in the context of FilteredMeasure 'measureA')," +
          "Reference 'filtered' is of incompatible type (only Aggregated measures are allowed in the context of FilteredMeasure 'measureB')"
      }

      "fail when any filter references non-existent field or filter in filtered measure references another measure" in {

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds") {
             |
             |  measure aggregated(column = "sales.aggregated")
             |  measure calculated (calculation = "1 + 2")
             |  measure filtered(filter = "calculated > 1", column = "sales.filtered")
             |
             |  measure measureA(filter = "filtered > 1", column = "sales.measureA")
             |  measure measureB(filter = "calculated > 1", column = "sales.measureB")
             |  measure measureC(filter = "aggregated > 1", column = "sales.measureC")
             |  measure measureD(filter = "i_do_not_exist > 1", column = "sales.measureD")
             |
             |  table sales {
             |   column count
             |   column measureA
             |   column measureB
             |   column measureC
             |   column measureD
             |   column aggregated
             |  }
             |  filter "calculated > 1 and i_do_not_exist > 1"
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Measure filtered referencing non-existent table column sales.filtered," +
          "default filter contains missing refs [i_do_not_exist],"+
          "filtered measure 'filtered' references another measures [calculated] in filter," +
          "filtered measure 'measureA' references another measures [filtered] in filter," +
          "filtered measure 'measureB' references another measures [calculated] in filter," +
          "filtered measure 'measureC' references another measures [aggregated] in filter," +
          "measure 'measureD' filter references [i_do_not_exist] not found"
      }

      "fail when calculated measure references another calculated measure in calculation" in {

        val foodmartPsl =
          s"""schema Foodmart{
             |  measure calculated (calculation = "1 + 2")
             |  measure calculatedX (calculation = "calculated + 2")
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ), Nil).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Reference 'calculated' is of incompatible type (only Aggregated and Filtered measures are allowed in the context of NumericExpression in Calculated measure 'calculatedX')"
      }

      "fail when filtered measure is internally incompatible" in {
        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds", strict = false) {
             |
             |  dimension D(table = "sales"){
             |    level L
             |  }
             |
             |  measure viewsCount(column = "views.count")
             |  measure filtered(measure = "viewsCount",  filter = "D.L > 1")
             |
             |  table sales
             |
             |  table views {
             |   column count
             |  }
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(
            Map("Foodmart" -> foodmartPsl),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "measure 'filtered' refers to the incompatible set of fields [viewsCount,D.L]"
      }

      "fail when there is no table refs for dimensions or measures" in {

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds") {
             |  measure count
             |  dimension A{
             |   level l {
             |     attribute x
             |    }
             |  }
             |  // TODO: do we want to support the dimensions without attributes or levels?
             |  dimension B
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "measure 'count' is not referenced from any table," +
          "attribute 'A.l' is not referenced from any table," +
          "attribute 'A.l.x' is not referenced from any table"
      }

      "fail when imported schema not found" in {

        val foodmartPsl =
          s"""schema Foodmart {
             |  import GA
             |  measure count
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ), Nil).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "schema GA not found"
      }

      "fail when data source not found" in {

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds") {
             |  measure count
             |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ), Nil).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "schema data source 'ds' not found"
      }

      "fail when trying to build table implicitly but there is no default datasource in schema" in {

        val foodmartPsl = """schema Foodmart(strict = false) {
           |  dimension A (table = "X"){
           |    attribute c
           |  }
           |}
          """.stripMargin

        intercept[Exception](
          new InMemoryCatalog(Map(
            "Foodmart" -> foodmartPsl
          ),Nil).getSchema("Foodmart").get).getMessage shouldBe "Schema parsing or compilation was unsuccessful: " +
          "Table references [X] are not found in tables and no datasource is defined on Schema level to create new ones"
      }

    }

    "conforming dimension from several schemas" should {
      "intersect in case of empty base dimension" in {
        val gaPsl =
          s"""|schema GoogleAnalytics(dataSource = "${ds.name}", strict = false) {
             |  dimension Time (table = "T"){
             |    attribute year
             |    attribute month
             |    attribute day
             |    attribute date
             |  }
             |}
             |""".stripMargin

        val fbPsl =
          s"""|schema FacebookStats(dataSource = "${ds.name}", strict = false) {
             |  dimension Time(table = "T2") {
             |    attribute year
             |    attribute month
             |    attribute week
             |    attribute date
             |  }
             |   table T2
             |}
             |""".stripMargin

        val foodmartPsl =
          """|schema Foodmart(strict = false) {
             |  import {GoogleAnalytics => ga, FacebookStats => fb}
             |
             |  dimension Date {
             |    conforms ga.Time
             |    conforms fb.Time
             |  }
             |}
             |""".stripMargin

        val combinedSchema = new InMemoryCatalog(Map(
                                                   "GoogleAnalytics" -> gaPsl,
                                                   "FacebookStats" -> fbPsl,
                                                   "Foodmart" -> foodmartPsl
                                                 ),
          List(ds)).getSchema("Foodmart").get

        assert(combinedSchema.getDimension("Date").get.conforms.isEmpty, "")
        combinedSchema.getDimensionAttribute("Date.year").get.conforms.size shouldBe 2
        combinedSchema.getDimensionAttribute("Date.date").get.conforms.size shouldBe 2
        assert(combinedSchema.getDimensionAttribute("Date.day").isEmpty)
        assert(combinedSchema.getDimensionAttribute("Date.week").isEmpty)
        combinedSchema.dimensions.map(_.name) should contain only("fb.Time", "ga.Time", "Date")
      }

      "combine in case of non-empty base dimension" in {
        val gaPsl =
          s"""|schema GoogleAnalytics(dataSource = "${ds.name}", strict = false) {
             |  dimension Time(table = "T") {
             |    attribute year
             |    attribute month
             |    attribute day
             |    attribute date
             |  }
             |  table T
             |}
             |""".stripMargin

        val fbPsl =
          s"""|schema FacebookStats(dataSource = "${ds.name}", strict = false) {
             |  dimension Time(table = "T") {
             |    attribute year
             |    attribute month
             |    attribute week
             |    attribute date
             |  }
             |  table T
             |}
             |""".stripMargin

        val foodmartPsl =
          s"""|schema Foodmart(dataSource = "${ds.name}", strict = false) {
             |  import {GoogleAnalytics => ga, FacebookStats => fb}
             |
             |  dimension Date(table = "T") {
             |    conforms ga.Time
             |    conforms fb.Time
             |
             |    attribute year
             |    attribute day
             |    attribute weekday
             |    attribute date
             |  }
             |  table T
             |}
             |""".stripMargin

        val combinedSchema = new InMemoryCatalog(Map(
                                                   "GoogleAnalytics" -> gaPsl,
                                                   "FacebookStats" -> fbPsl,
                                                   "Foodmart" -> foodmartPsl
                                                 ),
          List(ds)).getSchema("Foodmart").get

        assert(combinedSchema.getDimension("Date").get.conforms.isEmpty, "")
        combinedSchema.getDimensionAttribute("Date.year").get.conforms.size shouldBe 2
        combinedSchema.getDimensionAttribute("Date.day").get.conforms.size shouldBe 1
        combinedSchema.getDimensionAttribute("Date.weekday").get.conforms.size shouldBe 0
        combinedSchema.getDimensionAttribute("Date.date").get.conforms.size shouldBe 2
        assert(combinedSchema.getDimensionAttribute("Date.month").isEmpty)
        assert(combinedSchema.getDimensionAttribute("Date.week").isEmpty)
      }
    }

    "compatibility queries" should {
      "return all measures and dimensions with empty input" in {
        getFields(foodmartSchema.getAggregateCompatibility(Set())).size shouldBe 53
      }

      "return no dimensions and measures with incompatible dimensions" in {
        getFields(foodmartSchema.getAggregateCompatibility(Set("sales.Customer.province", "inv.Store.region.country"))) shouldBe empty
      }

      "return no dimensions and measures with incompatible dimension and measures" in {
        getFields(
          foodmartSchema.getAggregateCompatibility(
            Set("sales.Customer.province", "inv.Store.region.country", "inv.warehouseCost", "sales.unitSales")
          )) shouldBe empty
      }

      "return compatible dimensions and measures with dimension only query from imported schema" in {
        getFields(foodmartSchema.getAggregateCompatibility(Set("sales.Customer.province", "sales.Date.Year"))).size shouldBe 35
      }

      "return compatible dimensions and measures with dimension only query from root schema" in {
        getFields(foodmartSchema.getAggregateCompatibility(Set("Store.region.city", "Date.Year"))).size shouldBe 53
      }

      "return compatible dimensions and measures with dimensions and measures query" in {
        getFields(foodmartSchema.getAggregateCompatibility(Set("Store.region.city", "Date.Year", "sales", "warehouseSales"))).size shouldBe 18
      }

      "filtered and calculated measures compatibility" in {

        val schema = {
          val body =
            s"""schema Foodmart (dataSource = "foodmart") {
                 | measure storeSales(column = "sales_fact_1998.store_sales")
                 | measure storeSales1(column = "sales_fact_1997.store_sales")
                 |
                 | measure storeSales2{
                 |  conforms storeSales
                 |  conforms storeSales1
                 | }
                 |
                 | measure f1(measure="storeSales2", filter = "Customer.lname = 'Joe'")
                 | measure f2(measure="storeSales2", filter = "Time.year = 1")
                 |
                 | measure calc1 (calculation="f1 + 1")
                 | measure calc2 (calculation="f2 + 1")
                 |
                 | dimension Time (table ="time_by_day"){
                 |  attribute year(column="the_year")
                 | }
                 | dimension Customer (table ="customer"){
                 |  attribute lname(column="lname")
                 | }
                 |table time_by_day{
                 |    column time_id
                 |     column the_year
                 |}
                 |table customer{
                 |    column customer_id
                 |    column lname
                 |}
                 |table sales_fact_1998 {
                 |   column customer_id(tableRef = "customer.customer_id")
                 |   column store_sales
                 |}
                 |table sales_fact_1997 {
                 |  column time_id(tableRef = "time_by_day.time_id")
                 |   column store_sales
                 |}
                 |}""".stripMargin

          new InMemoryCatalog(Map("_test_" -> body), List(ds)).getSchema("_test_").get
        }

        val compat1 = schema.getAggregateCompatibility(Set("storeSales2", "Time.year"))

        compat1.dimensionAttributes.map(
          d =>
            (d.reference,
             d.importedFromSchema.getOrElse(""),
             d.dimensionName,
             d.hierarchyName,
             d.levelName,
             d.attribute.name)) shouldBe Set(
          ("Time.year", "", "Time", "", "", "year")
        )
        compat1.measures.map(m => (m.importedFromSchema.getOrElse(""), m.name, m.measure.name, m.reference)) shouldBe Set(
          ("", "storeSales2", "storeSales2", "storeSales2"),
          ("", "storeSales1", "storeSales1", "storeSales1"),
          ("", "f2", "f2", "f2"),
          ("", "calc2", "calc2", "calc2")
        )

        val compat2 = schema.getAggregateCompatibility(Set("storeSales", "storeSales2", "Customer.lname")) //shouldBe 2

        compat2.dimensionAttributes.map(
          d =>
            (d.reference,
             d.importedFromSchema.getOrElse(""),
             d.dimensionName,
             d.hierarchyName,
             d.levelName,
             d.attribute.name)) shouldBe Set(
          ("Customer.lname", "", "Customer", "", "", "lname")
        )
        compat2.measures.map(m => (m.importedFromSchema.getOrElse(""), m.name, m.measure.name, m.reference)) shouldBe Set(
          ("", "storeSales", "storeSales", "storeSales"),
          ("", "storeSales2", "storeSales2", "storeSales2"),
          ("", "f1", "f1", "f1"),
          ("", "calc1", "calc1", "calc1")
        )
      }

      "result in response containing unprefixed fields in case of wildcard exports(with excludes respected)" in {

        val xPsl = """schema X {
                      |  measure size(column = "salesX.size")
                      |
                      |  dimension D{
                      |    attribute color(table = "salesX")
                      |  }
                      |
                      |  table salesX(dataSource = "ds") {
                      |   column id
                      |   column size
                      |   column color
                      |  }
                      |
                      |}
                    """.stripMargin
        val gaPsl = """schema GA {
                      |  measure sessions(column = "sales.sessions")
                      |  measure sizeOfSmth(column = "sales.size")
                      |  dimension DD{
                      |    attribute color(table = "sales")
                      |  }
                      |
                      |  table sales(dataSource = "ds") {
                      |   column id
                      |   column sessions
                      |   column size
                      |   column color
                      |  }
                      |}
                    """.stripMargin

        val foodmartPsl =
          s"""schema Foodmart(dataSource = "ds") {
             |  export GA._
             |  // import is for testing of excludes
             |  import X._
             |
             |  measure count(column = "sales2.count")

             |  table sales2 {
             |   column id(tableRefs = ["sales.id", "salesX.id"])
             |   column count
             |  }
             |}
          """.stripMargin


          val schema = new InMemoryCatalog(Map(
            "GA" -> gaPsl,
            "X" -> xPsl,
            "Foodmart" -> foodmartPsl
          ),
            List(ReflectiveDataSource("ds", ()))).getSchema("Foodmart").get

        getFields(
          schema.getCompatibility(AggregateQuery(List("count")))
          // sizeOfSmth and DD.color are not excluded despite that they start with the exclued 'size' and 'D' respectively
        ) shouldBe Set("count", "sizeOfSmth", "sessions", "DD.color")

      }
    }

    "entity compatibility queries" should {
      "return all measures and dimensions with empty input" in {
        getFields(foodmartSchema.getEntityCompatibility(Set())).size shouldBe 46
      }

      "return matching dimensions with empty input when filtering dimensions" in {
        getFields(foodmartSchema.getEntityCompatibility(Set(), List("sales.", "inv."))).size shouldBe 35
      }

      "return no dimensions with incompatible dimensions" in {
        getFields(foodmartSchema.getEntityCompatibility(Set("sales.Customer.province", "inv.Store.region.country"))) shouldBe empty
      }

      "return no dimensions with incompatible dimension only query from imported schema" in {
        getFields(foodmartSchema.getEntityCompatibility(Set("sales.Customer.province", "sales.Date.Year"))).size shouldBe 0
      }

      "return matching dimensions with dimension only query from imported schema" in {
        getFields(foodmartSchema.getEntityCompatibility(Set("sales.Customer.province"))).size shouldBe 9
      }

      "return compatible dimensions and measures with dimension only query from root schema" in {
        getFields(foodmartSchema.getEntityCompatibility(Set("Store.region.city"))).size shouldBe 24
      }
    }
  }
}
