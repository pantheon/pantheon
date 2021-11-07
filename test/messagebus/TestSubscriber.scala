package messagebus

import messagebus.MessageBus.Message

import scala.collection.mutable
import scala.concurrent.Future

class TestSubscriber extends (Message => Future[Boolean]) {
  var messages: mutable.ArrayBuffer[Message] = mutable.ArrayBuffer.empty

  override def apply(m: Message): Future[Boolean] = {
    messages += m
    Future.successful(true)
  }
}
