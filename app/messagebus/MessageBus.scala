package messagebus

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import MessageBus.{Message, Subscriber}
import play.api.libs.json._

import scala.concurrent.Future

object MessageBus extends StrictLogging {

  sealed trait Message
  case class CancelQuery(catalogId: UUID, id: UUID) extends Message

  implicit val msgFormat = {
    implicit val ccFormat: Format[CancelQuery] = Json.format[CancelQuery]
    Json.format[Message]
  }

  type Processed = Boolean
  type Subscriber = Message => Future[Processed]
}

//Message bus implementation must process all incoming messages using Subscriber provided via AppLoader.MessageBusProvider
trait MessageBus {
  val subscriber: Subscriber
  def publish(m: Message): Future[Unit]
}
