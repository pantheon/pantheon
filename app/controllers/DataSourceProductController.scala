package controllers

import java.util.UUID

import cats.syntax.either._
import _root_.util.JsonSerializers.writesEnsuringFields
import cats.data.EitherT
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons.{ResourceId, ifAuthorized}
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}
import controllers.DataSourceProductController.{DataSourceProductListResponse, DataSourceProductReq, reqRespFormat}
import dao.Tables.DataSourceProductsRow
import play.api.libs.json._
import play.api.mvc.ControllerComponents
import play.api.mvc.Results.Ok
import services.Authorization.{ActionType, ResourceType}
import services.{Authorization, DataSourceProductRepo, DataSourceRepo}
import services.DataSourceProductRepo.DataSourceProductProperty

import scala.concurrent.Future

object DataSourceProductController {

  object DataSourceProductReq {

    def fromRow(row: DataSourceProductsRow, props: Seq[DataSourceProductProperty] = Seq.empty): DataSourceProductReq =
      DataSourceProductReq(
        row.isBundled,
        row.productRoot,
        row.className,
        Some(row.dataSourceProductId),
        row.name,
        row.`type`,
        row.description,
        props
      )
  }

  case class DataSourceProductReq(
      isBundled: Boolean = false,
      productRoot: String,
      className: Option[String] = None,
      id: Option[java.util.UUID] = None,
      name: String,
      `type`: String,
      description: Option[String] = None,
      properties: Seq[DataSourceProductProperty] = Seq.empty
  ) {

    def toRow: DataSourceProductsRow = DataSourceProductsRow(
      isBundled = isBundled,
      productRoot = productRoot,
      className = className,
      dataSourceProductId = id.getOrElse(UUID.randomUUID()),
      name = name,
      `type` = `type`,
      description = description
    )
  }

  implicit val reqRespFormat = {
    implicit val propFormat = Json.format[DataSourceProductProperty]
    val reqReads = Json.reads[DataSourceProductReq]
    val resWrites = writesEnsuringFields(Json.writes[DataSourceProductReq], "className", "name", "description")
    OFormat(reqReads, resWrites)
  }
  case class DataSourceProductListResponse(page: Page, data: Seq[DataSourceProductReq])
      extends PagedResponse[DataSourceProductReq]
}

class DataSourceProductController(components: ControllerComponents,
                                  action: PantheonActionBuilder,
                                  auth: Authorization,
                                  dspRepo: DataSourceProductRepo,
                                  dsRepo: DataSourceRepo)
    extends CrudlController[DataSourceProductReq, None](components,
                                                        action,
                                                        auth,
                                                        ResourceType.DataSourceProduct,
                                                        None,
                                                        _.id.get) {

  def list[DataSourceProductListResponse](page: Option[Int], pageSize: Option[Int]) =
    action(
      r =>
        _ =>
          actions.list(DataSourceProductListResponse, r.user, page, pageSize, None)(
            dspRepo.list().map(_.map((DataSourceProductReq.fromRow _).tupled))))

  def find(dspId: ResourceId) =
    action(r =>
      _ => actions.find(r.user, dspId, None)(dspRepo.find(dspId).map(_.map((DataSourceProductReq.fromRow _).tupled))))

  def delete(dspId: ResourceId) =
    action(r => _ => actions.delete(r.user, dspId, None)(dspRepo.delete(dspId).map(Right(_))))

  def create =
    action(parse.json[DataSourceProductReq])(
      r =>
        _ =>
          actions.create(r.user, None)(
            dspRepo
              .create(r.body.toRow, r.body.properties)
              .map(_.map(row => DataSourceProductReq.fromRow(row, r.body.properties)))))

  def update(dspId: ResourceId) =
    action(parse.json[DataSourceProductReq])(
      r =>
        _ =>
          actions.update(r.user, dspId, None)(
            dspRepo
              .update(r.body.copy(id = Some(dspId)).toRow, r.body.properties)
              .map(res => Right(res.map(row => DataSourceProductReq.fromRow(row, r.body.properties))))))

  def testConnection(dspId: ResourceId) =
    action(parse.json[Map[String, String]])(
      r =>
        _ =>
          auth
            .checkPermission(r.user, ResourceType.DataSourceProduct, ActionType.Read, None, dspId)
            .flatMap(
              ifAuthorized(
                dsRepo
                  .testConnection(dspId, r.body)
                  .map(
                    _.fold(idNotFound(dspId, None)(actions.resourceIdShow))(res =>
                      Ok(Json.toJson(ConnectionTestResult.fromEither(res))))
                  )))
    )

}
