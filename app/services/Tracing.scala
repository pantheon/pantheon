package services

import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.SamplerConfiguration
import io.jaegertracing.internal.{JaegerSpan, JaegerTracer}
import io.jaegertracing.internal.samplers.ConstSampler
import io.jaegertracing.spi.{Reporter, Sender}
import io.opentracing.{Span, Tracer}

object Tracing {
  case class JaegerTracerConfig(service: String, host: String, port: Int, sync: Boolean)

  // SyncReporter reports every span to the sender synchronously.
  // It is helpful for debugging but should probably not be used in production.
  class SyncReporter(sender: Sender) extends Reporter {
    def report(span: JaegerSpan): Unit = sender.append(span)

    def close(): Unit = {
      sender.flush
      sender.close
    }
  }

  type TraceStarter = String => (Tracer, Span)

  def traceStarter(tc: JaegerTracerConfig): TraceStarter = { desc: String =>
    val configFromEnv = Configuration.fromEnv(tc.service)
    val sender = configFromEnv.getReporter.getSenderConfiguration.withAgentHost(tc.host).withAgentPort(tc.port)

    // ConstSampler "withParam 1" reports every span, see SamplerConfiguration for details of other sample methods
    val sampler = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1)
    val config = configFromEnv
      .withSampler(sampler)
      .withReporter(configFromEnv.getReporter.withSender(sender))

    val tracerBuilder =
      if (tc.sync) config.getTracerBuilder.withReporter(new SyncReporter(sender.getSender)) else config.getTracerBuilder
    val tracer = tracerBuilder.build
    val span = tracer.buildSpan(desc).startActive(false).span()
    (tracer, span)
  }
}
