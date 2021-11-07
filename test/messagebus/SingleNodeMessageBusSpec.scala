package messagebus

import java.util.UUID

import messagebus.MessageBus.CancelQuery
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually, scaled}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.ExecutionContext.global

class SingleNodeMessageBusSpec extends WordSpec with MustMatchers {

  implicit val pc = new PatienceConfig(scaled(Span(5, Seconds)), scaled(Span(10, Millis)))

  "SingleNodeMessageBus" must {
    "deliver messages to processor" in {
      val processor = new TestSubscriber()
      val bus = new SingleNodeMessageBus(processor, global)
      val msg = CancelQuery(UUID.randomUUID(), UUID.randomUUID())

      bus.publish(msg)
      bus.publish(msg)
      // Implementation may be async in future
      eventually(processor.messages mustBe Seq(msg, msg))
    }
  }
}
