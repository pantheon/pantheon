package management

import java.util.UUID

import enumeratum.{Enum, EnumEntry}
import pantheon.backend.BackendPlan
import pantheon.planner.QueryPlan
import services.serializers.QuerySerializers.{QueryReq, QueryReqBase}

sealed trait QueryHistoryRecordType extends EnumEntry
object QueryHistoryRecordType extends Enum[QueryHistoryRecordType] {
  case object Native extends QueryHistoryRecordType
  case object Schema extends QueryHistoryRecordType
  override def values = findValues
}
sealed trait QueryEnvelope
case class Native(query: String, dataSourceId: UUID) extends QueryEnvelope
case class SchemaBased(query: QueryReqBase, schemaId: UUID) extends QueryEnvelope

case class QueryDescription(query: QueryEnvelope, catalogId: UUID, customReference: Option[String])
case class Plans(plan: Option[QueryPlan] = None, backendLogicalPlan: BackendPlan, backendPhysicalPlan: BackendPlan)
