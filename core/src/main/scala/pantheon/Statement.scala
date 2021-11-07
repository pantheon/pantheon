package pantheon

import scala.language.higherKinds
import pantheon.planner._
import pantheon.util.Logging._
import pantheon.backend.{BackendPlan, BackendStatement}

import scala.util.Try

sealed trait StatementWithPlan extends Statement {
  val plan: QueryPlan
}

sealed trait Statement extends ContextLogging {

  val backendStatement: BackendStatement

  protected val ctx: LoggingContext

  lazy val backendLogicalPlan: BackendPlan =
    backendStatement.logicalPlan

  def backendPhysicalPlan: BackendPlan =
    backendStatement.physicalPlan

  def execute(): RowSet =
    logger.traceTime("Query execution") {
      backendStatement.execute()
    }(ctx)

  def cancel(): Try[Unit] = backendStatement.cancel
}

class AggregateStatement(connection: Connection, val query: AggregateQuery, params: Map[String, Expression])(
    implicit protected val ctx: LoggingContext)
    extends StatementWithPlan {
  logger.debug(s"Aggregate statement created with query: $query")

  lazy val backendStatement: BackendStatement =
    connection.backendConnection.createStatement(plan)

  lazy val plan: QueryPlan = {
    val planner = new Planner(connection.schema)
    val rules = substituteBindVariables(params) :: planner.rules
    planner.execute(QueryPlan.fromQuery(query, connection.schema.defaultFilter), rules)
  }
}

class RecordStatement(connection: Connection, val query: RecordQuery, params: Map[String, Expression])(
    implicit protected val ctx: LoggingContext)
    extends StatementWithPlan {

  logger.debug(s"Entity statement created with query: $query")

  lazy val plan: QueryPlan = {
    val planner = new Planner(connection.schema)
    val rules = substituteBindVariables(params) :: planner.rules
    planner.execute(QueryPlan.fromQuery(query, connection.schema.defaultFilter), rules)
  }

  lazy val backendStatement: BackendStatement =
    connection.backendConnection.createStatement(plan)
}

class SqlStatement(connection: Connection, val query: String)(implicit protected val ctx: LoggingContext)
    extends Statement {
  logger.debug(s"SqlStatement created with query: $query")
  lazy val backendStatement: BackendStatement = connection.backendConnection.createSqlStatement(query)
}

class PivotedStatement(connection: Connection, val query: AggregateQuery, params: Map[String, Expression])(
    implicit protected val ctx: LoggingContext)
    extends Statement {

  logger.debug(s"PivotedStatement created with query: $query")
  override val backendStatement: BackendStatement = new PivotedBackendStatement(connection, query, params)
}
