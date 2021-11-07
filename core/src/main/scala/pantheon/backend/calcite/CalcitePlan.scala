package pantheon.backend.calcite

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import pantheon.backend.BackendPlan

class CalcitePlan(val rel: RelNode) extends BackendPlan {
  override def toString: String = RelOptUtil.toString(rel)
}
