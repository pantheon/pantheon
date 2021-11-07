package controllers

import com.zaxxer.hikari.HikariConfig
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.schema.FoodmartSchemaFixture
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers.{GET, route}
import services.SchemaRepo.ParsedPsl
import util.Fixtures
import play.api.test.Helpers._

class TablesIntegrationSpec extends Fixtures with BaseOneAppPerSuite with FoodmartSchemaFixture{

  "TablesController" must {
    "return the tables with columns from schema" in {

      val cid = createCatalog().catalogId
      val ds1Id = createFoodmartDS(cid, "DS1").dataSourceId
      val ds2Id = createFoodmartDS(cid, "DS2").dataSourceId
      val q3 = "\"\"\""
      val schema =
        s"""schema Inventory (dataSource = "DS1", strict = false) {
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
          |    level warehouse(columns = ["warehouse_id", "inventory_FOO.warehouse_id"]) {
          |      attribute name(column = "warehouse_name")
          |    }
          |  }
          |
          |  measure warehouseSales(column = "inventory_FOO.warehouse_sales")
          |  measure warehouseCost(column = "inventory_FOO.warehouse_cost")
          |
          |  table store {
          |    column region_id (tableRef = "region.region_id")
          |  }
          |
          |  table StoreExpr (sql=$q3 select * from "foodmart"."store" $q3) {
          |    columns *
          |    column region_id (tableRef = "region.region_id")
          |  }
          |
          |  table inventory_FOO (dataSource = "DS2", physicalTable = "inventory_fact_1998") {
          |    columns *
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

      val schemaId = createSchema(cid, ParsedPsl(schema).right.get, Seq(ds1Id, ds2Id)).schemaId

      val res = route(app, FakeRequest(GET, s"/catalogs/$cid/schemas/$schemaId/tables")).get

      // order of columns in tables may affect this test
      contentAsJson(res) mustBe  Json.parse("""
           |[ {
           |  "name" : "time_by_day",
           |  "columns" : [ "the_year", "the_month", "the_date", "time_id" ]
           |}, {
           |  "name" : "region",
           |  "columns" : [ "sales_region", "sales_state_province", "sales_city", "sales_country", "region_id" ]
           |}, {
           |  "name" : "warehouse",
           |  "columns" : [ "warehouse_country", "warehouse_city", "warehouse_id", "warehouse_name" ]
           |}, {
           |  "name" : "store",
           |  "columns" : [ "region_id", "store_id", "store_name", "store_type" ]
           |}, {
           |  "name" : "StoreExpr",
           |  "columns" : [ "region_id", "store_id", "store_type", "store_name", "store_number", "store_street_address", "store_city", "store_state", "store_postal_code", "store_country", "store_manager", "store_phone", "store_fax", "first_opened_date", "last_remodel_date", "store_sqft", "grocery_sqft", "frozen_sqft", "meat_sqft", "coffee_bar", "video_store", "salad_bar", "prepared_food", "florist" ]
           |}, {
           |  "name" : "inventory_FOO",
           |  "columns" : [ "time_id", "store_id", "warehouse_id", "warehouse_sales", "warehouse_cost", "product_id", "units_ordered", "units_shipped", "supply_time", "store_invoice" ]
           |} ]
           |""".stripMargin)
    }

    "return the tables with columns from data source" in {

      val cid = createCatalog().catalogId
      val ds1Id = createFoodmartDS(cid, "DS1").dataSourceId

      val res = route(app, FakeRequest(GET, s"/catalogs/$cid/dataSources/$ds1Id/tables")).get

     contentAsJson(res) mustBe Json.parse(""" [{
                           |  "name" : "employee_closure",
                           |  "columns" : [ "employee_id", "supervisor_id", "distance" ]
                           |}, {
                           |  "name" : "sales_fact_1998",
                           |  "columns" : [ "product_id", "time_id", "customer_id", "promotion_id", "store_id", "store_sales", "store_cost", "unit_sales" ]
                           |}, {
                           |  "name" : "employee",
                           |  "columns" : [ "employee_id", "full_name", "first_name", "last_name", "position_id", "position_title", "store_id", "department_id", "birth_date", "hire_date", "end_date", "salary", "supervisor_id", "education_level", "marital_status", "gender", "management_role" ]
                           |}, {
                           |  "name" : "store",
                           |  "columns" : [ "store_id", "store_type", "region_id", "store_name", "store_number", "store_street_address", "store_city", "store_state", "store_postal_code", "store_country", "store_manager", "store_phone", "store_fax", "first_opened_date", "last_remodel_date", "store_sqft", "grocery_sqft", "frozen_sqft", "meat_sqft", "coffee_bar", "video_store", "salad_bar", "prepared_food", "florist" ]
                           |}, {
                           |  "name" : "department",
                           |  "columns" : [ "department_id", "department_description" ]
                           |}, {
                           |  "name" : "salary",
                           |  "columns" : [ "pay_date", "employee_id", "department_id", "currency_id", "salary_paid", "overtime_paid", "vacation_accrued", "vacation_used" ]
                           |}, {
                           |  "name" : "expense_fact",
                           |  "columns" : [ "store_id", "account_id", "exp_date", "time_id", "category_id", "currency_id", "amount" ]
                           |}, {
                           |  "name" : "time_by_day",
                           |  "columns" : [ "time_id", "the_date", "the_day", "the_month", "the_year", "day_of_month", "week_of_year", "month_of_year", "quarter", "fiscal_period" ]
                           |}, {
                           |  "name" : "reserve_employee",
                           |  "columns" : [ "employee_id", "full_name", "first_name", "last_name", "position_id", "position_title", "store_id", "department_id", "birth_date", "hire_date", "end_date", "salary", "supervisor_id", "education_level", "marital_status", "gender" ]
                           |}, {
                           |  "name" : "promotion",
                           |  "columns" : [ "promotion_id", "promotion_district_id", "promotion_name", "media_type", "cost", "start_date", "end_date" ]
                           |}, {
                           |  "name" : "store_ragged",
                           |  "columns" : [ "store_id", "store_type", "region_id", "store_name", "store_number", "store_street_address", "store_city", "store_state", "store_postal_code", "store_country", "store_manager", "store_phone", "store_fax", "first_opened_date", "last_remodel_date", "store_sqft", "grocery_sqft", "frozen_sqft", "meat_sqft", "coffee_bar", "video_store", "salad_bar", "prepared_food", "florist" ]
                           |}, {
                           |  "name" : "warehouse_class",
                           |  "columns" : [ "warehouse_class_id", "description" ]
                           |}, {
                           |  "name" : "customer",
                           |  "columns" : [ "customer_id", "account_num", "lname", "fname", "mi", "address1", "address2", "address3", "address4", "city", "state_province", "postal_code", "country", "customer_region_id", "phone1", "phone2", "birthdate", "marital_status", "yearly_income", "gender", "total_children", "num_children_at_home", "education", "date_accnt_opened", "member_card", "occupation", "houseowner", "num_cars_owned", "fullname" ]
                           |}, {
                           |  "name" : "region",
                           |  "columns" : [ "region_id", "sales_city", "sales_state_province", "sales_district", "sales_region", "sales_country", "sales_district_id" ]
                           |}, {
                           |  "name" : "currency",
                           |  "columns" : [ "currency_id", "date", "currency", "conversion_ratio" ]
                           |}, {
                           |  "name" : "category",
                           |  "columns" : [ "category_id", "category_parent", "category_description", "category_rollup" ]
                           |}, {
                           |  "name" : "position",
                           |  "columns" : [ "position_id", "position_title", "pay_type", "min_scale", "max_scale", "management_role" ]
                           |}, {
                           |  "name" : "inventory_fact_1998",
                           |  "columns" : [ "product_id", "time_id", "warehouse_id", "store_id", "units_ordered", "units_shipped", "warehouse_sales", "warehouse_cost", "supply_time", "store_invoice" ]
                           |}, {
                           |  "name" : "account",
                           |  "columns" : [ "account_id", "account_parent", "account_description", "account_type", "account_rollup", "Custom_Members" ]
                           |}, {
                           |  "name" : "product_class",
                           |  "columns" : [ "product_class_id", "product_subcategory", "product_category", "product_department", "product_family" ]
                           |}, {
                           |  "name" : "days",
                           |  "columns" : [ "day", "week_day" ]
                           |}, {
                           |  "name" : "warehouse",
                           |  "columns" : [ "warehouse_id", "warehouse_class_id", "stores_id", "warehouse_name", "wa_address1", "wa_address2", "wa_address3", "wa_address4", "warehouse_city", "warehouse_state_province", "warehouse_postal_code", "warehouse_country", "warehouse_owner_name", "warehouse_phone", "warehouse_fax" ]
                           |}, {
                           |  "name" : "product",
                           |  "columns" : [ "product_class_id", "product_id", "brand_name", "product_name", "SKU", "SRP", "gross_weight", "net_weight", "recyclable_package", "low_fat", "units_per_case", "cases_per_pallet", "shelf_width", "shelf_height", "shelf_depth" ]
                           |} ]""".stripMargin)

      // checking that it is possible to run native query with given fields
      val queryRes = route(app, FakeRequest(POST, s"/catalogs/$cid/dataSources/$ds1Id/nativeQuery")
        .withBody(Json.obj("query" -> """select sum("store_sales") as "sumSales", avg("store_sales") avgSales from "sales_fact_1998""""))).get

      // TODO: we should not send measureHeaders and measures in the response to native query!!
      contentAsJson(queryRes) mustBe Json.parse(
        """{"columns":[{"ref":"sumSales","primitive":"number"},{"ref":"AVGSALES","primitive":"number"}],"rows":[[1079147.47,6.5578]],"measureHeaders":[],"measures":[]}""".stripMargin)
    }

    "connection leak detection for /dataSources/.../tables" in {
      val cid = createCatalog().catalogId
      val ds1Id = createFoodmartDS(cid, "DS1").dataSourceId

      testLeak {
        val res = route(app, FakeRequest(GET, s"/catalogs/$cid/dataSources/$ds1Id/tables")).get
        status(res) mustBe OK
      }
    }

    "connection leak detection for /dataSources/.../schemas" in {
      val cid = createCatalog().catalogId
      val ds1Id = createFoodmartDS(cid, "DS1").dataSourceId
      val schema =
        s"""schema Inventory (dataSource = "DS1") {
           |  table time_by_day {
           |    columns *
           |  }
           |}
           |""".stripMargin

      val schemaId = createSchema(cid, ParsedPsl(schema).right.get, Seq(ds1Id)).schemaId

      testLeak {
        val res = route(app, FakeRequest(GET, s"/catalogs/$cid/schemas/$schemaId/tables")).get
        status(res) mustBe OK
      }
    }
  }
}
