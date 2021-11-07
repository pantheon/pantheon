package controllers

import java.util.UUID

import cats.data.EitherT
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons.ResourceId
import controllers.SavedQueryController.{SavedQueryListResponse, SavedQueryResponse}
import controllers.helpers.CrudlActions.{Page, PagedResponse, pageWrites}
import pantheon.planner.BindVariable
import play.api.libs.json.{JsObject, Json, Writes}
import play.api.mvc.{ControllerComponents, Result}
import services.Authorization.ResourceType
import services.CatalogRepo.CatalogId
import services.SavedQuery.savedQueryFormat
import services._

import scala.concurrent.Future

object SavedQueryController {
  type SavedQueryRequest = SavedQuery

  // Special serializer needed for response (reimplement with 'newtype in future)
  class SavedQueryResponse(val savedQuery: SavedQuery)

  case class SavedQueryListResponse(page: Page, data: Seq[SavedQueryResponse]) extends PagedResponse[SavedQueryResponse]
  // Adding dynamic field 'requestSchema' to the response of endpoints
  implicit val savedQueryListResponseWrites: Writes[SavedQueryResponse] = savedQueryResp => {
    val sq = savedQueryResp.savedQuery

    val requestSchema = {

      val bindVariables: Set[String] =
        (sq.query.filter ++ sq.query.aggregateFilter)
          .flatMap(_.collect { case BindVariable(name) => name })(collection.breakOut)

      val bindVariablesPart: JsObject = JsObject(
        // TODO: this is wrong (not all types == string). Will be fixed when types are added to PSL.
        bindVariables.map(_ -> Json.obj("type" -> "string"))(collection.breakOut)
      )

      val systemVariablesPart = JsObject(
        // TODO: use libraries like https://github.com/Opetushallitus/scala-schema to autogenerate JSON schema from SavedQueryExecController.QueryParams
        SavedQueryExecController.systemKeys.map(k =>
          k -> (k match {
            case "_page" =>
              Json.obj("type" -> "integer", "minimum" -> 1, "format" -> "int32", "system" -> true)
            case "_pageSize" =>
              Json.obj("type" -> "integer", "minimum" -> 1, "format" -> "int32", "system" -> true)
            case "_queryId" =>
              Json.obj("type" -> "string", "format" -> "uuid", "system" -> true)
            case "_customReference" =>
              Json.obj("type" -> "string", "maxLength" -> 255, "system" -> true)
            case unexpected =>
              throw new AssertionError(
                s"Unexpected system key $unexpected. This code is out of sync with SavedQueryExecController.QueryParams."
              )
          }))(collection.breakOut)
      )

      Json.obj(
        "type" -> "object",
        "properties" -> (bindVariablesPart ++ systemVariablesPart),
        "required" -> bindVariables
      )
    }

    savedQueryFormat.writes(sq) + ("requestSchema" -> requestSchema)
  }
}
class SavedQueryController(components: ControllerComponents,
                           action: PantheonActionBuilder,
                           auth: Authorization,
                           sqRepo: SavedQueryRepo,
                           chkCatalogExists: UUID => EitherT[Future, Result, Unit])
    extends CrudlController[SavedQueryResponse, Some](components,
                                                      action,
                                                      auth,
                                                      ResourceType.SavedQuery,
                                                      Some(chkCatalogExists),
                                                      _.savedQuery.id) {

  import SavedQueryController._

  def list(catId: CatalogId, page: Option[Int], pageSize: Option[Int]) =
    action(
      r =>
        _ =>
          actions.list(SavedQueryListResponse, r.user, page, pageSize, Some(catId))(
            sqRepo.list(catId).map(_.map(new SavedQueryResponse(_)))))

  def find(catId: CatalogId, sqId: ResourceId) =
    action(
      r => _ => actions.find(r.user, sqId, Some(catId))(sqRepo.find(catId, sqId).map(_.map(new SavedQueryResponse(_)))))

  def delete(catId: CatalogId, sqId: ResourceId) =
    action(r => _ => actions.delete(r.user, sqId, Some(catId))(sqRepo.delete(catId, sqId)))

  def create(catId: CatalogId) =
    action(parse.json[SavedQuery])(
      r =>
        _ =>
          actions.create(r.user, Some(catId))(
            sqRepo.create(r.body.copy(catalogId = catId)).map(_.map(new SavedQueryResponse(_)))))

  def update(catId: CatalogId, sqId: ResourceId) =
    action(parse.json[SavedQuery])(
      r =>
        _ =>
          actions.update(r.user, sqId, Some(catId))(
            sqRepo.update(r.body.copy(catalogId = catId, id = sqId)).map(_.map(_.map(new SavedQueryResponse(_))))))
}
