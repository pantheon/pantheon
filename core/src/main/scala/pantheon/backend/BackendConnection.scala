package pantheon.backend

import pantheon.planner.QueryPlan
import pantheon.schema.Table
import pantheon.util.Logging.LoggingContext

import scala.concurrent.Future

trait BackendConnection {
  def createSqlStatement(query: String)(implicit ctx: LoggingContext): BackendStatement
  def createStatement(plan: QueryPlan)(implicit ctx: LoggingContext): BackendStatement
  def close(implicit ctx: LoggingContext): Unit

  // this operation is assumed to do (async) side effects
  def verifyTable(table: Table)(implicit ctx: LoggingContext): Future[List[String]]
}
