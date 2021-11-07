package controllers

import java.util.UUID

import _root_.util.JsonSerializers.writesEnsuringFields
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons.ResourceId
import dao.Tables.SchemasRow
import play.api.libs.json._
import play.api.mvc.{ControllerComponents, Result}
import services.Authorization.ResourceType
import services.CatalogRepo.CatalogId
import services._
import SchemaController._
import cats.data.EitherT
import cats.syntax.either._

import scala.concurrent.Future
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}

object SchemaController {

  object SchemaReq {
    def fromRow(row: SchemasRow): SchemaReq = SchemaReq(
      Some(row.name),
      row.description,
      row.psl,
      Some(row.catalogId),
      row.backendConfigId,
      Some(row.schemaId)
    )
  }

  case class SchemaReq(name: Option[String] = None,
                       description: Option[String] = None,
                       psl: String,
                       catalogId: Option[java.util.UUID] = None,
                       backendConfigId: Option[java.util.UUID] = None,
                       id: Option[java.util.UUID] = None) {

    def toRow: SchemasRow = SchemasRow(
      name.getOrElse(""),
      description,
      psl,
      catalogId.getOrElse(UUID.randomUUID()),
      backendConfigId,
      id.getOrElse(UUID.randomUUID())
    )
  }

  implicit val reqEntityReads: Reads[SchemaReq] = Json.reads[SchemaReq]
  implicit val respEntityWrites: OWrites[SchemaReq] =
    writesEnsuringFields(Json.writes[SchemaReq], "description")

  case class SchemaListResponse(page: Page, data: Seq[SchemaReq]) extends PagedResponse[SchemaReq]
}

class SchemaController(components: ControllerComponents,
                       action: PantheonActionBuilder,
                       auth: Authorization,
                       sr: SchemaRepo,
                       chkCatalogExists: UUID => EitherT[Future, Result, Unit])
    extends CrudlController[SchemaReq, Some](components,
                                             action,
                                             auth,
                                             ResourceType.Schema,
                                             Some(chkCatalogExists),
                                             _.id.get) {

  import SchemaController._

  def list(catId: CatalogId, page: Option[Int], pageSize: Option[Int]) =
    action(
      r =>
        _ =>
          actions.list(SchemaListResponse, r.user, page, pageSize, Some(catId))(
            sr.list(catId).map(_.map(s => SchemaReq.fromRow(s).copy(psl = "schema ...")))))

  def find(catId: CatalogId, schemaId: ResourceId) =
    action(
      r => _ => actions.find(r.user, schemaId, Some(catId))(sr.find(catId, schemaId).map(_.map(SchemaReq.fromRow))))

  def delete(catId: CatalogId, schemaId: ResourceId) =
    action(r => _ => actions.delete(r.user, schemaId, Some(catId))(sr.delete(catId, schemaId)))

  def create(catId: CatalogId) =
    action(parse.json[SchemaReq])(
      // TODO: open issue in scalafmt repo about newlines between curried params
      r =>
        _ =>
          actions.create(r.user, Some(catId))(
            sr.create(r.body.copy(catalogId = Some(catId)).toRow).map(_.map(SchemaReq.fromRow))))

  def update(catId: CatalogId, schemaId: ResourceId) =
    action(parse.json[SchemaReq])(
      r =>
        _ =>
          actions.update(r.user, schemaId, Some(catId))(
            sr.update(r.body.copy(catalogId = Some(catId), id = Some(schemaId)).toRow)
              .map(_.map(_.map(SchemaReq.fromRow)))))
}
