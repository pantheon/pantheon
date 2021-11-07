package controllers

import java.util.UUID

import cats.syntax.either._
import config.PantheonActionBuilder
import dao.Tables._
import play.api.libs.json.{Json, OFormat}
import play.api.mvc.ControllerComponents
import services.Authorization.ResourceType
import services.CatalogRepo.CatalogId
import services.{Authorization, CatalogRepo}
import util.JsonSerializers.writesEnsuringFields
import CatalogController._
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}

object CatalogController {

  object CatalogReq {
    def fromRow(row: CatalogsRow): CatalogReq = CatalogReq(
      row.name,
      row.description,
      Some(row.catalogId),
      row.backendConfigId
    )
  }

  case class CatalogReq(name: Option[String] = None,
                        description: Option[String] = None,
                        id: Option[java.util.UUID] = None,
                        backendConfigId: Option[java.util.UUID] = None) {

    def toRow: CatalogsRow = CatalogsRow(
      name,
      description,
      id.getOrElse(UUID.randomUUID()),
      backendConfigId
    )
  }

  implicit val reqRespFormat = {
    val reqReads = Json.reads[CatalogReq]
    val respWrites =
      writesEnsuringFields(Json.writes[CatalogReq], "name", "description", "backendConfigId")

    OFormat(reqReads, respWrites)
  }

  case class CatalogListResponse(page: Page, data: Seq[CatalogReq]) extends PagedResponse[CatalogReq]
}

class CatalogController(components: ControllerComponents,
                        action: PantheonActionBuilder,
                        auth: Authorization,
                        cd: CatalogRepo)
    extends CrudlController[CatalogReq, None](components, action, auth, ResourceType.Catalog, None, _.id.get) {

  // TODO: type parameter of PagedResponse is not described in swagger.json
  def list(page: Option[Int], pageSize: Option[Int]) =
    action(
      r => _ => actions.list(CatalogListResponse, r.user, page, pageSize, None)(cd.list.map(_.map(CatalogReq.fromRow))))

  def find(catId: CatalogId) =
    action(r => _ => actions.find(r.user, catId, None)(cd.find(catId).map(_.map(CatalogReq.fromRow))))

  def delete(catId: CatalogId) = action(r => _ => actions.delete(r.user, catId, None)(cd.delete(catId)))

  def create =
    action(parse.json[CatalogReq])(r =>
      _ => actions.create(r.user, None)(cd.create(r.body.toRow).map(_.map(CatalogReq.fromRow))))

  def update(catId: CatalogId) =
    action(parse.json[CatalogReq])(
      r =>
        _ =>
          actions.update(r.user, catId, None)(
            cd.update(r.body.copy(id = Some(catId)).toRow).map(res => Right(res.map(CatalogReq.fromRow)))))

}
