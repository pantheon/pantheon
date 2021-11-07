package pantheon.backend.spark

import org.apache.spark.sql.catalyst.plans.QueryPlan
import pantheon.backend.BackendPlan

class SparkPlan(plan: QueryPlan[_]) extends BackendPlan {
  override def toString: String = plan.toString()
  override def canonical: String = plan.canonicalized.toString
}
