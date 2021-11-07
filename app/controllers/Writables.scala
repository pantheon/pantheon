package controllers

import akka.util.ByteString
import play.api.http.{ContentTypes, Writeable}
import play.api.libs.json.{Json, Writes}

object Writables {

  implicit def jsonWritable[T](implicit w: Writes[T]): Writeable[T] = Writeable(
    (v: T) => ByteString(Json.stringify(w.writes(v))),
    Some(ContentTypes.JSON)
  )
}
