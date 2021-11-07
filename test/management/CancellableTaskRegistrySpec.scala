package management

import java.time.Instant
import java.util.UUID
import java.util.concurrent._

import management.CancellableTasksRegistry._
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually, scaled}
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Success, Try}
import pantheon.util.Logging

class CancellableTaskRegistrySpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  //calling futures with infinitely blocking tasks, making sure more threads will be spawned and killing them after all tests.
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  implicit val loggingContext = Logging.newContext

  def secondsBeforeNow(t: Instant) = java.time.Duration.between(t, Instant.now()).getSeconds

  override def afterAll(): Unit = ec.shutdownNow()

  class CancellableTask {
    @volatile var cancelled = false
    @volatile var started = false

    private val intervalMillis: Int = 20

    def finiteExec(f: Future[Unit]): Unit = {
      started = true
      Await.result(f, 10.days)
    }

    def inifiniteExec() = {
      started = true
      while (true) {
        Thread.sleep(intervalMillis)
        if (cancelled) throw new Exception("Task was cancelled")
      }
    }

    def cancel(): Try[Unit] = {
      cancelled = true
      Success(())
    }

  }

  def genId() = UUID.randomUUID()
  "CancellableTasksRegistry" must {

    type Description = Int

    val someDecription: Description = 42

    implicit val pc = new PatienceConfig(scaled(Span(30, Seconds)), scaled(Span(10, Millis)))

    "register task and then cancel it" in {

      val reg = new CancellableTasksRegistry[Description]
      val taskDef = new CancellableTask()
      val taskId = genId()
      val task = Future(reg.run(someDecription, taskDef.cancel(), taskId)(taskDef.inifiniteExec()))

      eventually(taskDef.started mustBe (true))
      reg.cancel(taskId) mustBe Success(true)
      whenReady(task)(_ mustBe Left(Cancelled))
      taskDef.cancelled mustBe true
    }

    "set 3 tasks, cancel one, complete other, 3-rd stays running" in {
      val reg = new CancellableTasksRegistry[Description]
      val (taskDef1, task1Id) = (new CancellableTask(), genId())
      val taskDef2 = new CancellableTask()
      val taskDef3 = new CancellableTask()

      val toBeCancelled = Future(reg.run(1, taskDef1.cancel(), task1Id)(taskDef1.inifiniteExec()))
      val staysRunning = Future(reg.run(2, taskDef2.cancel(), genId())(taskDef2.inifiniteExec()))
      val task3promise = Promise[Unit]
      val completes = Future(reg.run(3, taskDef3.cancel(), genId())(taskDef3.finiteExec(task3promise.future)))

      eventually(taskDef1.started mustBe (true))
      eventually(taskDef2.started mustBe (true))
      eventually(taskDef3.started mustBe (true))

      reg.cancel(task1Id) mustBe Success(true)
      task3promise.success(())

      whenReady(toBeCancelled)(_ mustBe Left(Cancelled))
      whenReady(completes)(_ mustBe Right(()))
      staysRunning.isCompleted mustBe false
    }

    "no-op cancel function: task stays registered as running after cancel is invoked" in {
      val reg = new CancellableTasksRegistry[Description]
      val taskDef = new CancellableTask()

      val noOpCancel = Success(())
      val taskId = genId()
      val registered = Future(reg.run(someDecription, noOpCancel, taskId)(taskDef.inifiniteExec()))

      eventually(taskDef.started mustBe (true))
      reg.cancel(taskId) mustBe Success(true)
      taskDef.cancelled mustBe false
      registered.isCompleted mustBe false
    }

    "no-op cancel function, task function throws exception after cancel is invoked, task completes as Canceled" in {
      val reg = new CancellableTasksRegistry[Description]
      val taskDef = new CancellableTask()

      val p = Promise[Unit]
      val noOpCancel = Success(())
      val taskId = genId()
      val task = Future(reg.run(someDecription, noOpCancel, taskId)(taskDef.finiteExec(p.future)))

      eventually(taskDef.started mustBe (true))

      reg.cancel(taskId) mustBe Success(true)

      taskDef.cancelled mustBe false

      p.failure(new Exception("task failed"))

      whenReady(task)(_ mustBe Left(Cancelled))
    }

    "task exec function throws exception: task completes as failed" in {
      val reg = new CancellableTasksRegistry[Description]
      val taskDef = new CancellableTask()

      val err = new Exception("task failed")

      val willFail = Future(reg.run(someDecription, taskDef.cancel(), genId())(taskDef.finiteExec(Future(throw err))))

      whenReady(willFail)(_ mustBe Left(Failed(err)))
    }

    "support conditional cancel invocation" in {
      val reg = new CancellableTasksRegistry[Description]
      val taskDef = new CancellableTask()
      val id = genId()

      Future(reg.run(someDecription, taskDef.cancel(), id)(taskDef.inifiniteExec()))

      eventually(taskDef.started mustBe (true))

      reg.cancel(id, _ => false)
      taskDef.cancelled mustBe false

      reg.cancel(id, _ => true) mustBe Success(true)
      taskDef.cancelled mustBe true
    }
  }
}
