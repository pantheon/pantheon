package pantheon.backend.calcite

import java.util.function.Consumer
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.rules.{AggregateProjectMergeRule, JoinCommuteRule, JoinPushThroughJoinRule}
import org.apache.calcite.runtime.Hook
import pantheon.backend.Backend
import pantheon.schema.Schema

import java.sql.DriverManager

object CalciteBackend {

  DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());

  def apply(params: Map[String, String] = Map.empty): CalciteBackend = new CalciteBackend(params)

  {
    //noinspection ScalaDeprecation
    Hook.PLANNER.add(new Consumer[VolcanoPlanner] {
      override def accept(planner: VolcanoPlanner) = {
        planner.getRules.forEach {
          // this rule is excluded to avoid cyclic planning process (CALCITE-2015)
          case r: JoinCommuteRule => planner.removeRule(r)

          // this rule is excluded because it removes aliases from fields
          case r: AggregateProjectMergeRule => planner.removeRule(r)

          case _ =>
        }
      }
    })
  }
}

class CalciteBackend(params: Map[String, String]) extends Backend {
  override def getConnection(schema: Schema): CalciteConnection =
    CalciteConnection(schema.tables.map(_.dataSource), Some(schema), params)
}
