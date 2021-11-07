package messagebus

import java.util.UUID

import messagebus.MessageBus.CancelQuery
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually, scaled}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{MustMatchers, WordSpec}
import dimodules.AppTestConfig

import scala.concurrent.ExecutionContext.global

class RedisMessageBusSpec extends WordSpec with MustMatchers with AppTestConfig {

  implicit val pc = new PatienceConfig(scaled(Span(5, Seconds)), scaled(Span(10, Millis)))

  if (config.getBoolean("redis.enabled")) {
    "RedisMessageBus" must {

      "deliver messages to processor" in {
        val processor = new TestSubscriber()

        val bus = new RedisMessageBus(
          "localhost",
          config.getInt("redis.port "),
          config.getString("redis.message.channel "),
          processor,
          global
        )
        val msg = CancelQuery(UUID.randomUUID(), UUID.randomUUID())

        bus.publish(msg)
        bus.publish(msg)

        eventually(processor.messages mustBe Seq(msg, msg))
      }
    }
  }
}
