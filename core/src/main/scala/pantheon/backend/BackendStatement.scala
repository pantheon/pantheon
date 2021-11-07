package pantheon.backend

import pantheon.RowSet

import scala.util.Try

trait BackendStatement {
  def logicalPlan: BackendPlan
  def physicalPlan: BackendPlan
  def execute(): RowSet
  def cancel(): Try[Unit]
}
