package pantheon.schema

import pantheon.{DataSource, InMemoryCatalog, JdbcDataSource}

trait SparkFoodmartSchemaFixture {

//  DataSource.create("foodmart", "hsqldb-foodmart", k)
  val ds = JdbcDataSource(
    "foodmart",
    "jdbc:hsqldb:res:foodmart",
    Map(
      "user" -> "FOODMART",
      "password" -> "FOODMART",
      "default_schema" -> "foodmart",
      "jdbcSchema" -> "foodmart",
      "driver" -> "org.hsqldb.jdbc.JDBCDriver"
    ),
    None
  )

  val salesSchema =
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
      |  table region {
      |    column region_id
      |    column sales_city
      |    column sales_state_province
      |    column sales_region
      |    column sales_country
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
      |    column birthYear(expression = "year(birthdate)")
      |    column gender
      |  }
      |
      |  table store {
      |    column store_id
      |    column region_id (tableRef = "region.region_id")
      |    column store_name
      |    column store_type
      |  }
      |
      |  table sales_fact_1998 {
      |    column time_id (tableRef = "time_by_day.time_id")
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
      |  table region {
      |    column region_id
      |    column sales_city
      |    column sales_state_province
      |    column sales_region
      |    column sales_country
      |  }
      |
      |  table store {
      |    column store_id
      |    column region_id (tableRef = "region.region_id")
      |    column store_name
      |    column store_type
      |  }
      |
      |  table warehouse {
      |    column warehouse_id
      |    column warehouse_country
      |    column warehouse_city
      |    column warehouse_name
      |  }
      |
      |  table inventory_fact_1998 {
      |    column time_id (tableRef = "time_by_day.time_id")
      |    column store_id (tableRef = "store.store_id")
      |    column warehouse_id (tableRef = "warehouse.warehouse_id")
      |
      |    column warehouse_sales
      |    column warehouse_cost
      |  }
      |}
      |""".stripMargin

  val rootSchema =
    """schema Foodmart(dataSource = "foodmart") {
      |  export { Sales => sales }
      |  export { Inventory => inv }
      |
      |  dimension Date {
      |    conforms sales.Date
      |    conforms inv.Date
      |  }
      |
      |  dimension Store {
      |    level region {
      |      conforms sales.Store.region
      |      conforms inv.Store.region
      |
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
      |
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

  val catalog = new InMemoryCatalog(Map(
                                      "Sales" -> salesSchema,
                                      "Inventory" -> invSchema,
                                      "Foodmart" -> rootSchema
                                    ),
                                    List(ds))
}
