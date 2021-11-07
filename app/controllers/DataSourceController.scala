package controllers

import java.net.URI
import java.util.UUID

import util.JsonSerializers.uriFormat
import _root_.util.JsonSerializers.writesEnsuringFields
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons.{ResourceId, ifAuthorized}
import dao.Tables.{DataSourcesRow, DataSourceProductsRow}
import play.api.libs.json._
import play.api.mvc.{Action, ControllerComponents, Result}
import services.Authorization.{ActionType, ResourceType}
import services.CatalogRepo.CatalogId
import services._
import DataSourceController._
import cats.data.EitherT
import cats.syntax.either._
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}
import play.api.mvc.Results.Ok
import pantheon.util.withResource

import scala.concurrent.Future
import scala.language.higherKinds

object DataSourceController {

  object DataSourceResp {
    def fromRowWithProducts(rowWithProducts: (DataSourcesRow, DataSourceProductsRow)): DataSourceResp = {
      val (row, product) = rowWithProducts
      DataSourceResp(
        row.name,
        product.name,
        row.description,
        row.properties,
        Some(row.catalogId),
        row.dataSourceProductId,
        Some(row.dataSourceId)
      )
    }
  }

  case class DataSourceReq(name: String,
                           description: Option[String] = None,
                           properties: Map[String, String],
                           catalogId: Option[java.util.UUID] = None,
                           dataSourceProductId: java.util.UUID,
                           id: Option[java.util.UUID] = None) {

    def toRow = DataSourcesRow(
      name,
      description,
      properties,
      catalogId.getOrElse(UUID.randomUUID()),
      dataSourceProductId,
      id.getOrElse(UUID.randomUUID())
    )
  }

  case class DataSourceResp(name: String,
                            dataSourceProductName: String,
                            description: Option[String] = None,
                            properties: Map[String, String],
                            catalogId: Option[java.util.UUID] = None,
                            dataSourceProductId: java.util.UUID,
                            id: Option[java.util.UUID] = None)

  implicit val reqReads: Reads[DataSourceReq] = Json.reads[DataSourceReq]
  implicit val respWrites: OWrites[DataSourceResp] =
    writesEnsuringFields(Json.writes[DataSourceResp], "description")

  case class DataSourceListResponse(page: Page, data: Seq[DataSourceResp]) extends PagedResponse[DataSourceResp]
}

class DataSourceController(components: ControllerComponents,
                           action: PantheonActionBuilder,
                           auth: Authorization,
                           dsd: DataSourceRepo,
                           chkCatalogExists: UUID => EitherT[Future, Result, Unit])
    extends CrudlController[DataSourceResp, Some](components,
                                                  action,
                                                  auth,
                                                  ResourceType.DataSource,
                                                  Some(chkCatalogExists),
                                                  _.id.get) {

  def list(catId: CatalogId, page: Option[Int], pageSize: Option[Int]) =
    action(
      r =>
        _ =>
          actions.list(DataSourceListResponse, r.user, page, pageSize, Some(catId))(
            dsd.list(catId).map(_.map(DataSourceResp.fromRowWithProducts))))

  def find(catId: CatalogId, dsId: ResourceId) =
    action(r =>
      _ =>
        actions.find(r.user, dsId, Some(catId))(dsd.find(catId, dsId).map(_.map(DataSourceResp.fromRowWithProducts))))

  def testConnection(catId: CatalogId, dsId: ResourceId) =
    action(
      r =>
        _ =>
          auth
            .checkPermission(r.user, ResourceType.DataSource, ActionType.Read, Some(catId), dsId)
            .flatMap(
              ifAuthorized(
                dsd
                  .getDataSource(catId, dsId)
                  .map(_.fold(idNotFound(dsId, catId))(ds =>
                    Ok(Json.toJson(ConnectionTestResult.fromEither(
                      Either.catchNonFatal(withResource(ds)(_.testConnection())).leftMap(_.getMessage)
                    ))))))))

  def delete(catId: CatalogId, dsId: ResourceId) =
    action(r => _ => actions.delete(r.user, dsId, Some(catId))(dsd.delete(catId, dsId)))

  def create(catId: CatalogId): Action[DataSourceReq] =
    action(parse.json[DataSourceReq])(
      r =>
        _ =>
          actions.create(r.user, Some(catId))(
            dsd.create(r.body.copy(catalogId = Some(catId)).toRow).map(_.map(DataSourceResp.fromRowWithProducts))))

  def update(catId: CatalogId, dsId: ResourceId): Action[DataSourceReq] =
    action(parse.json[DataSourceReq])(
      r =>
        _ =>
          actions.update(r.user, dsId, Some(catId))(
            dsd
              .update(r.body.copy(id = Some(dsId), catalogId = Some(catId)).toRow)
              .map(_.map(_.map(DataSourceResp.fromRowWithProducts)))))

}
