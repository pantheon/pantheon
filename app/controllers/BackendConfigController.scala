package controllers

import java.util.UUID

import config.PantheonActionBuilder
import dao.Tables.BackendConfigsRow
import play.api.libs.json._
import play.api.mvc.{ControllerComponents, Result}
import services.{Authorization, BackendConfigRepo}
import services.CatalogRepo.CatalogId
import _root_.util.JsonSerializers.writesEnsuringFields
import cats.data.EitherT
import services.Authorization.ResourceType
import cats.syntax.either._
import controllers.AuthorizationControllerCommons.ResourceId
import controllers.BackendConfigController.{BackendConfigListResponse, BackendConfigReq}
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}

import scala.concurrent.Future

object BackendConfigController {

  object BackendConfigReq {
    def fromRow(row: BackendConfigsRow): BackendConfigReq = BackendConfigReq(
      name = row.name,
      backendType = row.backendType,
      description = row.description,
      params = row.params,
      catalogId = Some(row.catalogId),
      id = Some(row.backendConfigId)
    )
  }

  case class BackendConfigReq(name: Option[String] = None,
                              backendType: String,
                              description: Option[String] = None,
                              params: Map[String, String],
                              catalogId: Option[java.util.UUID] = None,
                              id: Option[java.util.UUID] = None) {
    def toRow: BackendConfigsRow = BackendConfigsRow(
      name,
      backendType,
      description,
      params,
      catalogId.getOrElse(UUID.randomUUID()),
      id.getOrElse(UUID.randomUUID())
    )
  }

  implicit val reqRespFormat = {
    val reqReads = Json.reads[BackendConfigReq]
    val resWrites = writesEnsuringFields(Json.writes[BackendConfigReq], "name", "description")
    OFormat(reqReads, resWrites)
  }

  case class BackendConfigListResponse(page: Page, data: Seq[BackendConfigReq]) extends PagedResponse[BackendConfigReq]
}

class BackendConfigController(components: ControllerComponents,
                              action: PantheonActionBuilder,
                              auth: Authorization,
                              bcRepo: BackendConfigRepo,
                              chkCatalogExists: UUID => EitherT[Future, Result, Unit])
    extends CrudlController[BackendConfigReq, Some](components,
                                                    action,
                                                    auth,
                                                    ResourceType.BackendConfig,
                                                    Some(chkCatalogExists),
                                                    _.id.get) {

  def list(catId: CatalogId, page: Option[Int], pageSize: Option[Int]) =
    action(
      r =>
        _ =>
          actions.list(BackendConfigListResponse, r.user, page, pageSize, Some(catId))(
            bcRepo.list(catId).map(_.map(BackendConfigReq.fromRow))))

  def find(catId: CatalogId, id: ResourceId) =
    action(r => _ => actions.find(r.user, id, Some(catId))(bcRepo.find(catId, id).map(_.map(BackendConfigReq.fromRow))))

  def delete(catId: CatalogId, id: ResourceId) =
    action(r => _ => actions.delete(r.user, id, Some(catId))(bcRepo.delete(catId, id)))

  def create(catId: CatalogId) =
    action(parse.json[BackendConfigReq])(
      r =>
        _ =>
          actions.create(r.user, Some(catId))(
            bcRepo.create(r.body.toRow.copy(catalogId = catId)).map(_.map(BackendConfigReq.fromRow))))

  def update(catId: CatalogId, id: ResourceId) =
    action(parse.json[BackendConfigReq])(
      r =>
        _ =>
          actions.update(r.user, id, Some(catId))(
            bcRepo
              .update(r.body.toRow.copy(backendConfigId = id, catalogId = catId))
              .map(_.map(_.map(BackendConfigReq.fromRow)))))
}
