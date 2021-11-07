package pantheon.util

import java.io.Closeable

import com.typesafe.scalalogging.{CanLog, Logger, LoggerTakingImplicit}
import java.util.Random

import io.opentracing.{Span, Tracer}

import scala.concurrent.{ExecutionContext, Future}

object Logging {

  private val random: Random = new Random()

  class LoggingContext private[Logging] (val id: Int) {
    def withSpan[A](desc: String)(f: Span => A): A = f(null)
    def withChildSpan[A](desc: String, parent: Span)(f: Span => A): A = f(null)
    def withSpanOverFuture[A](desc: String)(f: Span => Future[A])(implicit ec: ExecutionContext): Future[A] = f(null)
    def finish(): Unit = Unit
  }

  class LoggingContextWithTracing private[Logging] (override val id: Int, root: Span, tracer: Tracer)
      extends LoggingContext(id) {
    override def withSpan[A](desc: String)(f: Span => A): A = {
      val span = startSpan(desc)
      try {
        f(span)
      } finally {
        span.finish()
      }
    }

    override def withChildSpan[A](desc: String, parent: Span)(f: Span => A): A = {
      val span = startSpan(desc, parent)
      try {
        f(span)
      } finally {
        span.finish()
      }
    }

    override def withSpanOverFuture[A](desc: String)(f: Span => Future[A])(implicit ec: ExecutionContext): Future[A] = {
      val span = startSpan(desc)
      f(span).andThen { case _ => span.finish() }
    }

    override def finish(): Unit = {
      root.finish()
      tracer match {
        case t: Closeable => t.close()
      }
    }

    private def startSpan(desc: String): Span = {
      tracer.buildSpan(desc).asChildOf(root).start
    }

    private def startSpan(desc: String, parent: Span): Span = {
      tracer.buildSpan(desc).asChildOf(parent).start
    }
  }

  def newContext: LoggingContext = {
    val rand = random.nextInt(Int.MaxValue)
    new LoggingContext(rand)
  }

  def newContextWithTracing(desc: String, traceStarter: String => (Tracer, Span)): LoggingContext = {
    val rand = random.nextInt(Int.MaxValue)
    val (tracer, span) = traceStarter(desc)
    new LoggingContextWithTracing(rand, span, tracer)
  }

  implicit val canLog: CanLog[LoggingContext] =
    (originalMsg: String, ctx: LoggingContext) => s"<LoggingCtx ${ctx.id}> $originalMsg"

  trait ContextLogging {
    protected val logger: LoggerTakingImplicit[LoggingContext] = Logger.takingImplicit(getClass)
  }

  implicit class LoggerOps(val logger: LoggerTakingImplicit[LoggingContext]) extends AnyVal {
    def traceTime[T](msg: String)(f: => T)(implicit ctx: LoggingContext): T = {
      if (logger.underlying.isDebugEnabled) {
        logger.debug(s"$msg started.")
        val start = System.currentTimeMillis()
        val result = f
        logger.debug(s"$msg finished. Total milliseconds: ${System.currentTimeMillis() - start}")
        result
      } else f
    }
  }
}
