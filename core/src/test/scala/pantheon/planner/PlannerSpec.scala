package pantheon.planner

import org.scalatest.{MustMatchers, WordSpec}
import pantheon.schema.DsProviders
import pantheon.{InMemoryCatalog, SortOrder}
import pantheon.schema.FoodmartSchemaFixture
import pantheon.util.Logging
import QueryPlan.pprint

class PlannerSpec extends WordSpec with MustMatchers {
  implicit val ctx = Logging.newContext

  val ds = DsProviders.defaultDs

  object f extends FoodmartSchemaFixture

  val catalog = new InMemoryCatalog(f.allSchemas, List(ds))

  "Query Planner" when {
    "correct foodmart plan" should {
      val schema = catalog
        .getSchema("Foodmart")
        .get
      val planner = new Planner(schema)

      "with filter" in {
        val query =
          SchemaProject(
            List(
              UnresolvedField("Date.Month"),
              UnresolvedField("Store.region.city"),
              UnresolvedField("sales.sales"),
              UnresolvedField("sales.cost")),
            SchemaAggregate(child =
              Filter(
                GreaterThan(UnresolvedField("Date.Year"), literal.Integer(2016)),
                UnresolvedSchema()
              )))

        val plan = planner.execute(query, planner.rules)
        pprint(plan) mustBe
          """Project[cols=[c(Date.Month), c(Store.region.city), c(sales.sales), c(sales.cost)]
            |  Aggregate[group=[c(Date.Month), c(Store.region.city)], aggregations=[Agg(Sum, sales.sales), Agg(Sum, sales.cost)]]
            |    Project[cols=[c(Date.Month), c(Store.region.city), c(sales.sales), c(sales.cost)]
            |      Filter[GreaterThan(ColumnRef(the_year,Some(sales.time_by_day)),Integer(2016))]
            |        Join[c(store_id) = c(store_id), type=Left]
            |          Join[c(time_id) = c(time_id), type=Inner]
            |            TableScan[sales.sales_fact_1998]
            |            TableScan[sales.time_by_day]
            |          Join[c(region_id) = c(region_id), type=Left]
            |            TableScan[sales.store]
            |            TableScan[sales.region]""".stripMargin
      }

    }
  }
}
