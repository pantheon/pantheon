package messagebus

import messagebus.MessageBus.Message

import scala.concurrent.{ExecutionContext, Future}

// TODO: executing event instead of publishing it may cause unwanted delays(not relevant currently).
class SingleNodeMessageBus(override val subscriber: Message => Future[Boolean], ec: ExecutionContext)
    extends MessageBus {
  override def publish(e: Message): Future[Unit] = subscriber(e).map(_ => ())(ec)
}
