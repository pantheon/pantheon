package messagebus

import com.redis._
import com.typesafe.scalalogging.StrictLogging
import messagebus.MessageBus.{Message, Subscriber}
import play.api.libs.json.Json

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class RedisMessageBus(
    host: String,
    port: Int,
    channel: String,
    override val subscriber: Subscriber,
    ec: ExecutionContext
) extends MessageBus
    with StrictLogging {

  // Redis does not allow to reuse connection for both pub ans sub.
  val subClient = new RedisClient(host, port)
  val pubClient = new RedisClient(host, port)

  override def publish(e: Message): Future[Unit] =
    Future.successful(pubClient.publish(channel, Json.toJson(e).toString))

  subClient.subscribe(channel) {

    val pubSubProcessor: PubSubMessage => Future[Boolean] = {
      case M(ch, msg) =>
        assert(channel == ch, s"Got message from unexpected channel $ch")

        val parsedMsg =
          Json
            .parse(msg)
            .validate[Message]
            .fold(
              err =>
                throw new AssertionError(
                  s"Message protocol is inconsistent. Cannot deserialize message from $msg. $err"),
              identity
            )

        subscriber(parsedMsg).recover {
          case t: Throwable =>
            logger.error("Message processing error", t)
            false
        }(ec)

      case E(t) =>
        logger.error("Got error message", t)
        Future.successful(false)

      case _: S | _: U => Future.successful(false)
    }

    msg =>
      // Processing messages sequentially.
      // Await is ok here because Redis client spawns separate thread that is not reusable anyway.
      Await.ready(pubSubProcessor(msg), Duration.Inf)
  }

}
