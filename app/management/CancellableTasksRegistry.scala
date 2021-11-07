package management

import java.util.UUID

import scala.collection.concurrent
import scala.util.{Success, Try}
import cats.instances.future._
import CancellableTasksRegistry._
import pantheon.util.Logging.{ContextLogging, LoggingContext}

import scala.concurrent.{ExecutionContext, blocking}
import scala.util.control.NonFatal

object CancellableTasksRegistry {

  sealed trait NotSuccessful
  case object Cancelled extends NotSuccessful
  case class Failed(error: Throwable) extends NotSuccessful
  case class DuplicateTaskId(id: UUID) extends NotSuccessful

  case class Entry[D](taskDef: D, cancel: () => Try[Unit])
}

/*
 *  Cancellation is detected by exception interception. This service is used to register synchronous queries.
 *  Calcite does not provide reliable method to detect that query was cancelled,
 *  it throws general SqlException with db-specific cause if query was cancelled on the phase of execution in DB
 */
class CancellableTasksRegistry[D] extends ContextLogging {

  private val runningCache = concurrent.TrieMap.empty[UUID, Entry[D]]
  private val cancelRequested: concurrent.TrieMap[UUID, Unit] = concurrent.TrieMap.empty

  def run[R](taskDef: D, cancel: => Try[Unit], taskId: UUID)(exec: => R)(
      implicit ctx: LoggingContext,
      ec: ExecutionContext
  ): Either[NotSuccessful, R] = {

    for {
      _ <- runningCache.put(taskId, Entry(taskDef, () => cancel)).map(_ => DuplicateTaskId(taskId)).toLeft(())

      res <- try {
        Right(blocking(exec))
      } catch {
        case NonFatal(e) =>
          Left(
            // Considering any failed task that was attempted to be cancelled as cancelled.
            if (cancelRequested.contains(taskId)) Cancelled
            else {
              logger.error(s"Task $taskDef failed", e)
              Failed(e)
            }
          )
      } finally {
        runningCache.remove(taskId)
        cancelRequested.remove(taskId)
      }
    } yield res
  }

  // returns true if  runningCache contains taskId and cancel is attempted
  def cancel(taskId: UUID, cond: D => Boolean = _ => true): Try[Boolean] = {
    def runCancel(e: Entry[D]) = {
      cancelRequested.update(taskId, ())
      val r = e.cancel()
      // need this check because task may complete(and perform caches cleanup) after we check that it still runs and before the invocation of 'cancelRequested.update'
      if (!runningCache.contains(taskId)) cancelRequested.remove(taskId)
      r
    }

    runningCache.get(taskId).filter(v => cond(v.taskDef)).map(runCancel(_).map(_ => true)).getOrElse(Success(false))
  }
}
