package pantheon.backend.calcite

import pantheon.backend.BackendStatement
import pantheon.util.Logging.{ContextLogging, LoggingContext}

import scala.util.{Failure, Try}

class CalciteStatement(connection: CalciteConnection, override val logicalPlan: CalcitePlan)(
    implicit ctx: LoggingContext)
    extends BackendStatement
    with ContextLogging {

  lazy val preparedStatement: CalcitePreparedStatement = connection.prepare(logicalPlan.rel)

  override def physicalPlan: CalcitePlan = ctx.withSpan("CalciteStatement#physicalPlan") { _ =>
    preparedStatement.physicalPlan
  }

  override def execute(): JdbcRowSet = ctx.withSpan("CalciteStatement#execute") { _ =>
    new JdbcRowSet(preparedStatement.execute())
  }

  override def cancel(): Try[Unit] =
    // Currently this has NO effect on queries running in DB or in memory (closing pantheon connection does not help to)
    //Try(preparedStatement.preparedStatement.cancel())
    Failure(new NotImplementedError("Cancellation of the Calcite queries is not implemented"))
}
