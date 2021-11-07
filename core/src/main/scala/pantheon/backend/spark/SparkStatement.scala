package pantheon.backend.spark

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import pantheon.backend.{BackendPlan, BackendStatement}

import scala.util.Try

class SparkStatement(val dataFrame: DataFrame, sparkContext: SparkContext) extends BackendStatement {

  private val groupId: String = UUID.randomUUID().toString

  @volatile private var cancelled: Boolean = false

  private def setJobGroupIfNotCancelled(): Unit = {
    if (cancelled) throw new Exception("Spark statement was cancelled")
    sparkContext.setJobGroup(groupId, "Spark statement")
  }

  override def logicalPlan: BackendPlan = {
    setJobGroupIfNotCancelled()
    new SparkPlan(dataFrame.queryExecution.analyzed)
  }

  override def physicalPlan: BackendPlan = {
    setJobGroupIfNotCancelled()
    new SparkPlan(dataFrame.queryExecution.executedPlan)
  }

  override def execute(): SparkRowSet = new SparkRowSet(dataFrame, () => setJobGroupIfNotCancelled())

  override def cancel(): Try[Unit] = {
    cancelled = true
    Try(sparkContext.cancelJobGroup(groupId))
  }
}
