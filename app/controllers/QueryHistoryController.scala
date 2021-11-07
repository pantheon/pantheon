package controllers

import java.time.Instant
import java.util.UUID

import play.api.libs.json._
import play.api.mvc.{ControllerComponents, Result}
import Writables.jsonWritable
import cats.Show
import config.PantheonActionBuilder
import messagebus.MessageBus.CancelQuery
import services.{Authorization, QueryHistoryRepo}
import dao.Tables.QueryHistoryRow
import QueryHistoryController.{QueryHistoryListResponse, QueryHistoryRecord, queryHistoryWrites}
import cats.data.EitherT
import controllers.AuthorizationControllerCommons.{chkAuthorized, ifAuthorized}
import services.Authorization.{ActionType, ResourceType}
import services.QueryHistoryRepo.CompletionStatus
import controllers.AuthorizationControllerCommons.ifAuthorized
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}
import services.Authorization.{ActionType, ResourceType, UnmanagedResourceType}
import services.QueryHistoryRepo.CompletionStatus
import services.serializers.QuerySerializers.queryReqFormat
import management.QueryHistoryRecordType
import cats.instances.future._
import services.serializers.QuerySerializers.QueryReq

import scala.concurrent.Future

object QueryHistoryController {
  object QueryHistoryRecord {
    def fromRow(row: QueryHistoryRow): QueryHistoryRecord = {

      val buildRecord: (UUID,
                        Instant,
                        Option[CompletionStatus],
                        Option[Instant],
                        Option[String],
                        Option[String],
                        Option[String],
                        UUID,
                        Option[String]) => QueryHistoryRecord =
        row.`type` match {
          case QueryHistoryRecordType.Schema =>
            SchemaBasedHistoryRecord(_, _, Json.parse(row.query).as[QueryReq], row.schemaId.get, _, _, _, _, _, _, _)
          case QueryHistoryRecordType.Native =>
            NativeHistoryRecord(_, _, row.query, row.dataSourceId.get, _, _, _, _, _, _, _)
        }

      buildRecord(
        row.queryHistoryId,
        row.startedAt,
        row.completionStatus,
        row.completedAt,
        row.plan,
        row.backendLogicalPlan,
        row.backendPhysicalPlan,
        row.catalogId,
        row.customReference
      )
    }
  }

  sealed trait QueryHistoryRecord {
    def id: java.util.UUID
    def startedAt: Instant
    def completionStatus: Option[CompletionStatus]
    def completedAt: Option[Instant]
    def plan: Option[String]
    def backendLogicalPlan: Option[String]
    def backendPhysicalPlan: Option[String]
    def catalogId: java.util.UUID
    def customReference: Option[String]
    def `type`: QueryHistoryRecordType
  }
  case class SchemaBasedHistoryRecord(
      id: java.util.UUID,
      startedAt: Instant,
      query: QueryReq,
      schemaId: java.util.UUID,
      completionStatus: Option[CompletionStatus] = None,
      completedAt: Option[Instant] = None,
      plan: Option[String] = None,
      backendLogicalPlan: Option[String] = None,
      backendPhysicalPlan: Option[String] = None,
      catalogId: java.util.UUID,
      customReference: Option[String] = None,
      `type`: QueryHistoryRecordType.Schema.type = QueryHistoryRecordType.Schema
  ) extends QueryHistoryRecord

  case class NativeHistoryRecord(
      id: java.util.UUID,
      startedAt: Instant,
      query: String,
      dataSourceId: java.util.UUID,
      completionStatus: Option[CompletionStatus] = None,
      completedAt: Option[Instant] = None,
      plan: Option[String] = None,
      backendLogicalPlan: Option[String] = None,
      backendPhysicalPlan: Option[String] = None,
      catalogId: java.util.UUID,
      customReference: Option[String] = None,
      `type`: QueryHistoryRecordType.Native.type = QueryHistoryRecordType.Native
  ) extends QueryHistoryRecord

  implicit val queryHistoryWrites: OWrites[QueryHistoryRecord] = {
    import _root_.util.JsonSerializers.enumEntryWrites
    val schema = Json.writes[SchemaBasedHistoryRecord]
    val native = Json.writes[NativeHistoryRecord]
    _ match {
      case r: SchemaBasedHistoryRecord => schema.writes(r)
      case r: NativeHistoryRecord      => native.writes(r)
    }
  }

  case class QueryHistoryListResponse(page: Page, data: Seq[QueryHistoryRecord])
      extends PagedResponse[QueryHistoryRecord]
  implicit val listRespWrites = Json.writes[QueryHistoryListResponse]
}

class QueryHistoryController(controllerComponents: ControllerComponents,
                             action: PantheonActionBuilder,
                             auth: Authorization,
                             historyRepo: QueryHistoryRepo,
                             publishCancelMsg: CancelQuery => Future[Unit],
                             chkCatalogExists: UUID => EitherT[Future, Result, Unit])
    extends PantheonBaseController(controllerComponents) {

  def list(catalogId: UUID, page: Option[Int], pageSize: Option[Int], customRefPattern: Option[String]) =
    action { request => implicit ctx =>
      import controllers.helpers.CrudlActions.pagedResponseWrites
      (for {
        pps <- EitherT.fromEither[Future](
          validatePageParamsWithDefault(page, pageSize)
        )
        (page, pageSize) = pps
        _ <- chkCatalogExists(catalogId)
        _ <- EitherT[Future, Result, Unit](
          // TODO: this should be a check on QueryHistory read permissions; not Catalog
          auth
            .checkPermission(request.user, ResourceType.Catalog, ActionType.Read, None, catalogId)
            .map(chkAuthorized)
        )
        res <- EitherT.liftF[Future, Result, (Int, Seq[QueryHistoryRow])](
          historyRepo.list(catalogId, page, pageSize, customRefPattern)
        )
        (allItemsCount, lst) = res
      } yield
        Ok(
          QueryHistoryListResponse(
            page = Page(page, pageSize, allItemsCount),
            data = lst.map(qh =>
              QueryHistoryRecord.fromRow(qh.copy(plan = None, backendLogicalPlan = None, backendPhysicalPlan = None)))
          ))).merge
    }

  def get(catalogId: UUID, qid: UUID) =
    action { request => _ =>
      auth
        .checkPermission(request.user, ResourceType.QueryHistory, ActionType.Read, Some(catalogId), qid)
        .flatMap(
          ifAuthorized {
            val idShow: Show[(UUID, UUID)] = {
              case (id, catId) => s"$id from catalog $catId"
            }

            historyRepo
              .find(catalogId, qid)
              .map(_.map(qh => Ok(QueryHistoryRecord.fromRow(qh))).getOrElse(idNotFound(qid -> catalogId)(idShow)))
          }
        )
    }

  def cancel(catalogId: UUID, qid: UUID) =
    action { request => _ =>
      auth
        .checkPermission(request.user, ResourceType.QueryHistory, ActionType.Edit, Some(catalogId), qid)
        .flatMap(ifAuthorized(publishCancelMsg(CancelQuery(catalogId, qid)).map(_ => Ok)))
    }
}
