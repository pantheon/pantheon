package services

import java.util.UUID

import play.api.libs.json._
import _root_.util.JsonSerializers.writesEnsuringFields
import _root_.util.JsonSerializers._
import services.serializers.QuerySerializers.QueryReq

case class SavedQuery(id: UUID,
                      catalogId: UUID,
                      schemaId: UUID,
                      query: QueryReq,
                      name: Option[String] = None,
                      description: Option[String] = None)

object SavedQuery {
  implicit val savedQueryFormat: OFormat[SavedQuery] = {
    OFormat(
      readsWithRandomDefaultIds(Json.reads[SavedQuery], "id", "catalogId"),
      writesEnsuringFields(Json.writes[SavedQuery], "name", "description")
    )
  }
}
