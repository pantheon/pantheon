package pantheon.schema

import pantheon.{DataSource, JdbcDataSource}
import pantheon.util.BoolOps
import com.typesafe.config.{Config, ConfigValueType}
import pantheon.dimodules.CoreTestConfig

import scala.collection.JavaConverters._
import cats.syntax.traverse._
import cats.syntax.foldable._
import cats.instances.either._
import cats.instances.option._
import cats.instances.map._
import cats.instances.string._
import cats.syntax.either._
import cats.instances.list._
import DataSourceProvider.readDataSourceProps
import enumeratum._
import pantheon.DataSource.DriverClassLoc

import scala.collection.immutable
import scala.reflect.io.File

object DataSourceProvider extends CoreTestConfig {
  val databasesConf = {
    val dbsPath = "test.dbs"
    assert(config.hasPath(dbsPath), "cannot find db config in test config file")
    config.getConfig(dbsPath)
  }

  // Discuss: Consider using one of existing scala wrappers for config(e.g.: pureConfig). It will eliminate boilerplate end reduce possibility of mistakes
  def readDataSourceProps(dsType: String): Option[Map[String, String]] = {

    def toMap(c: Config): Either[String, Map[String, String]] =
      c.root()
        .asScala
        .toList
        .filterNot(_._1 == "enabled")
        .traverse {
          case (k, v) if v.valueType() == ConfigValueType.STRING => Right(k -> v.unwrapped().asInstanceOf[String])
          case (k, v) if v.valueType() == ConfigValueType.NUMBER => Right(k -> v.unwrapped().toString)

          case (k, v) => Left(s"unsupported DS property of type ${v.valueType()}: '$k = ${v.render()}'")
        }
        .map(_.toMap)

    if (!databasesConf.hasPath(dsType)) {
      println(s"WARNING: db name '$dsType' not present in test config")
      None
    } else {
      val dbConf = databasesConf.getConfig(dsType)
      dbConf
        .getBoolean("enabled")
        .option(
          toMap(dbConf).valueOr(e => throw new AssertionError(s"$dsType: $e"))
        )
    }
  }
}

sealed class DataSourceProvider(jdbcDriverName: Option[String] = None) extends EnumEntry {
  def dsType: String = toString
  // props ad data source are both defined or both not defined
  private val propsAndDs: Option[(Map[String, String], DataSource)] = {

    // Reads all file paths from config/jdbc dir.
    def readClassPath(): String = {
      val driverDirName = dsType
      val driverDirPath = s"conf/jdbc/$driverDirName"
      val driverDirFile = File(driverDirPath)
      assert(
        driverDirFile.isDirectory && driverDirFile.canRead,
        s"driver dir $driverDirPath not found. Full resolved path=${driverDirFile.toAbsolute.path}"
      )
      val filesList = driverDirFile.toDirectory.list.map(_.toURL)
      assert(filesList.nonEmpty, s"no files in driver directory of ${dsType}")
      filesList.mkString(";")
    }

    readDataSourceProps(dsType)
      .map(
        props =>
          props ->
            DataSource
              .create(
                "foodmart",
                dsType,
                props ++ jdbcDriverName.foldMap(DriverClassLoc(_, Some(readClassPath())).toProps),
                None
              )
              .valueOr(e => throw new AssertionError(s"Failed to create '$dsType' datasource: $e")))
  }
  val propsOpt: Option[Map[String, String]] = propsAndDs.map(_._1)
  val dsOpt: Option[DataSource] = propsAndDs.map(_._2)
  def isEnabled: Boolean = propsAndDs.isDefined
  println(s"data source '$this' is ${dsOpt.fold("disabled")(_ => "enabled")}")
}

object DsProviders extends Enum[DataSourceProvider] {
  case object hsqldbFoodmart extends DataSourceProvider(Some("org.hsqldb.jdbc.JDBCDriver")) {
    override def dsType = "hsqldb-foodmart"
  }
  case object postgresql extends DataSourceProvider(Some("org.postgresql.Driver"))
  case object clickhouse extends DataSourceProvider(Some("ru.yandex.clickhouse.ClickHouseDriver"))
  case object mysql extends DataSourceProvider(Some("org.mariadb.jdbc.Driver"))
  case object hana extends DataSourceProvider(Some("com.sap.db.jdbc.Driver"))
  case object db2 extends DataSourceProvider(Some("com.ibm.db2.jcc.DB2Driver"))
  case object oracle extends DataSourceProvider(Some("oracle.jdbc.driver.OracleDriver"))
  case object teradata extends DataSourceProvider(Some("com.teradata.jdbc.TeraDriver"))

  val all: immutable.IndexedSeq[DataSourceProvider] = findValues
  val values = all

  def without(providers: DataSourceProvider*): Seq[DataSourceProvider] = all.diff(providers)

  val defaultProvider =
    all.find(_.dsOpt.isDefined).getOrElse(throw new AssertionError("No enabled data sources in test config file"))

  val defaultDs: DataSource = defaultProvider.dsOpt.get
}

trait FoodmartSchemaFixture {

  val salesSchema: String =
    """schema Sales (dataSource = "foodmart") {
      |
      |  dimension Date (table = "time_by_day") {
      |    level Year (column = "the_year")
      |    level Month (column = "the_month")
      |    level Date (column = "the_date")
      |  }
      |
      |  dimension Store(table = "store") {
      |    level region(column = "region_id", table = "region") {
      |      attribute city(column = "sales_city")
      |      attribute province(column = "sales_state_province")
      |      attribute region(column = "sales_region")
      |      attribute country(column = "sales_country")
      |    }
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
      |  table time_by_day {
      |    column time_id
      |    column the_year
      |    column the_month
      |    column the_date
      |  }
      |
      |  table customer {
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
      |  table store {
      |    columns *
      |    column region_id (tableRef = "region.region_id")
      |  }
      |
      |  table region {
      |    column region_id
      |    column sales_city
      |    column sales_state_province
      |    column sales_region
      |    column sales_country
      |  }
      |
      |  table sales_fact_1998 {
      |    column time_id {
      |      tableRef time_by_day.time_id (joinType = "inner")
      |    }
      |    column customer_id (tableRef = "customer.customer_id")
      |    column store_id (tableRef = "store.store_id")
      |
      |    column store_sales
      |    column store_cost
      |    column unit_sales
      |  }
      |
      |}
      |""".stripMargin

  val invSchema =
    """schema Inventory (dataSource = "foodmart") {
      |
      |  dimension Date (table = "time_by_day") {
      |    level Year (column = "the_year")
      |    level Month (column = "the_month")
      |    level Date (column = "the_date")
      |  }
      |
      |  dimension Store(table = "store") {
      |    level region(column = "region_id", table = "region") {
      |      attribute city(column = "sales_city")
      |      attribute province(column = "sales_state_province")
      |      attribute region(column = "sales_region")
      |      attribute country(column = "sales_country")
      |    }
      |    level store(column = "store_id") {
      |      attribute name(column = "store_name")
      |      attribute type(column = "store_type")
      |    }
      |  }
      |
      |  dimension Warehouse(table = "warehouse") {
      |    level country(column = "warehouse_country")
      |    level city(column = "warehouse_city")
      |    level warehouse(columns = ["warehouse_id", "inventory_fact_1998.warehouse_id"]) {
      |      attribute name(column = "warehouse_name")
      |    }
      |  }
      |
      |  measure warehouseSales(column = "inventory_fact_1998.warehouse_sales")
      |  measure warehouseCost(column = "inventory_fact_1998.warehouse_cost")
      |
      |  table time_by_day {
      |    column time_id
      |    column the_year
      |    column the_month
      |    column the_date
      |  }
      |
      |  table store {
      |    column store_id
      |    column store_name
      |    column store_type
      |    column region_id (tableRef = "region.region_id")
      |  }
      |
      |  table warehouse {
      |    column warehouse_id
      |    column warehouse_name
      |    column warehouse_city
      |    column warehouse_country
      |  }
      |
      |  table region {
      |    column region_id
      |    column sales_city
      |    column sales_state_province
      |    column sales_region
      |    column sales_country
      |  }
      |
      |  table inventory_fact_1998 {
      |    column time_id {
      |      tableRef time_by_day.time_id (joinType = "inner")
      |    }
      |    column store_id (tableRef = "store.store_id")
      |    column warehouse_id (tableRef = "warehouse.warehouse_id")
      |
      |    column warehouse_sales
      |    column warehouse_cost
      |  }
      |}
      |""".stripMargin

  val rootSchema =
    """schema Foodmart {
      |  export { Inventory => inv }
      |  export { Sales => sales }
      |
      |  dimension Date {
      |    conforms sales.Date
      |    conforms inv.Date
      |  }
      |
      |  dimension Store {
      |    level region  {
      |      conforms sales.Store.region
      |      conforms inv.Store.region
      |      attribute city {
      |        conforms sales.Store.region.city
      |        conforms inv.Store.region.city
      |      }
      |
      |      attribute province {
      |        conforms sales.Store.region.province
      |        conforms inv.Store.region.province
      |      }
      |
      |      attribute region {
      |        conforms sales.Store.region.region
      |        conforms inv.Store.region.region
      |      }
      |
      |      attribute country {
      |        conforms sales.Store.region.country
      |        conforms inv.Store.region.country
      |      }
      |    }
      |
      |    level store {
      |      conforms sales.Store.store
      |      conforms inv.Store.store
      |      attribute name {
      |        conforms sales.Store.store.name
      |        conforms inv.Store.store.name
      |      }
      |
      |      attribute type {
      |        conforms sales.Store.store.type
      |        conforms inv.Store.store.type
      |      }
      |    }
      |  }
      |
      |  measure sales {
      |    conforms sales.sales
      |  }
      |
      |  measure warehouseSales {
      |    conforms inv.warehouseSales
      |  }
      |
      |}
      |""".stripMargin

  val allSchemas: Map[String, String] = Map(
    "Sales" -> salesSchema,
    "Inventory" -> invSchema,
    "Foodmart" -> rootSchema
  )

  val q3 = "\"\"\""
  val fullFoodmart =
    raw"""
      |schema fullFoodmart(dataSource = "foodmart") {
      |  dimension Date (table = "time_by_day") {
      |    attribute year (column = "the_year")
      |    attribute quarter (column = "quarter")
      |    attribute month (column = "month_of_year")
      |    attribute week (column = "week_of_year")
      |    attribute day (column = "day_of_month")
      |    attribute date (column = "the_date")
      |  }
      |
      |  dimension Product(table = "product") {
      |    attribute brand(column = "brand_name")
      |    attribute name(column = "product_name")
      |    attribute weight(column = "weight_bucket")
      |    attribute recyclable_packaging(column = "recyclable_package")
      |  }
      |
      |  dimension Store(table = "store") {
      |    level region(column = "region_id", table = "region") {
      |      attribute city(column = "sales_city")
      |      attribute province(column = "sales_state_province")
      |      attribute region(column = "sales_region")
      |      attribute country(column = "sales_country")
      |    }
      |    level store(column = "store_id") {
      |      attribute name(column = "store_name")
      |      attribute type(column = "store_type")
      |    }
      |  }
      |
      |  dimension Customer(table = "customer") {
      |    attribute id(column = "customer_id")
      |    attribute name(column = "fname")
      |    attribute lastName(column = "lname")
      |    attribute birthDate(column = "birthdate")
      |    attribute address1(column = "address1")
      |    attribute address2(column = "address2")
      |    attribute address3(column = "address3")
      |    attribute birthYear
      |    attribute gender
      |  }
      |
      |  dimension Warehouse(table = "warehouse") {
      |    attribute name(column = "warehouse_name")
      |    attribute city(column = "warehouse_city")
      |    attribute country(column = "warehouse_country")
      |  }
      |  dimension WarehouseClass(table = "warehouse_class") {
      |    attribute description(column = "description")
      |  }
      |
      |  measure unitsOrdered(column = "inventory.units_ordered")
      |  measure unitsShipped(column = "inventory.units_shipped")
      |  measure warehouseSales(column = "inventory.warehouse_sales")
      |  measure warehouseCost(column = "inventory.warehouse_cost")
      |
      |  measure sales(column = "sales_fact.store_sales")
      |  measure cost(column = "sales_fact.store_cost")
      |  measure unitSales(column = "sales_fact.unit_sales")
      |
      |  table customer {
      |    column customer_id
      |    column birthYear(expression = "extract(year from birthdate)")
      |    column country
      |    column state_province
      |    column city
      |    column fname
      |    column lname
      |    column birthdate
      |    column gender
      |    column address1
      |    column address2
      |    column address3
      |  }
      |
      |  table store {
      |    column store_id
      |    column region_id (tableRef = "region.region_id")
      |    column store_name
      |    column store_type
      |  }
      |
      |  table region {
      |    column region_id
      |    column sales_city
      |    column sales_state_province
      |    column sales_region
      |    column sales_country
      |  }
      |
      |  table product {
      |    column product_id
      |    column brand_name
      |    column product_name
      |    column weight_bucket
      |    column recyclable_package
      |  }
      |
      |  table time_by_day {
      |    column time_id
      |    column the_year
      |    column quarter
      |    column month_of_year
      |    column week_of_year
      |    column day_of_month
      |    column the_date
      |  }
      |
      |  // using 'union all' because of https://github.com/contiamo/pantheon/issues/201
      |  table inventory(sql = $q3 select * from "foodmart"."inventory_fact_1998 $q3) {
      |    column time_id (tableRef = "time_by_day.time_id")
      |    column store_id (tableRef = "store.store_id")
      |    column product_id (tableRef = "product.product_id")
      |    column warehouse_id (tableRef = "warehouse.warehouse_id")
      |
      |    column units_ordered
      |    column units_shipped
      |    column warehouse_sales
      |    column warehouse_cost
      |  }
      |
      |  table sales_fact(sql = $q3 select * from "foodmart"."sales_fact_1998" $q3) {
      |    column time_id (tableRef = "time_by_day.time_id")
      |    column customer_id (tableRef = "customer.customer_id")
      |    column store_id (tableRef = "store.store_id")
      |    column product_id (tableRef = "product.product_id")
      |
      |    column store_sales
      |    column store_cost
      |    column unit_sales
      |  }
      |
      |  table warehouse {
      |    column warehouse_id
      |    column warehouse_class_id(tableRef = "warehouse_class.warehouse_class_id")
      |    column warehouse_name
      |    column warehouse_city
      |    column warehouse_country
      |  }
      |  table warehouse_class {
      |    column warehouse_class_id
      |    column description
      |  }
      |}
    """.stripMargin
}
