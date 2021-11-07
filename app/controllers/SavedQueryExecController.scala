package controllers

import java.util.UUID

import cats.data.EitherT
import pantheon.planner.{literal}
import play.api.mvc._
import play.api.libs.json._
import services.{Authorization, SavedQuery, SavedQueryRepo, SchemaRepo}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import cats.syntax.either._
import config.PantheonActionBuilder
import pantheon.util.Logging.ContextLogging
import Writables.jsonWritable
import cats.instances.future._
import data.serialization.literalReadsWithOptionalType
import controllers.AuthorizationControllerCommons.ifAuthorized
import play.api.libs.functional.~
import services.Authorization.{ActionType, ResourceType}
import services.serializers.QuerySerializers.{CustomRefsReqBase, QueryReq}
import _root_.util.JsonSerializers.pairReads

import scala.concurrent.Future
object SavedQueryExecController {

  type FilterParams = Map[String, literal.Literal]

  case class QueryParams(
      _page: Option[Int] = None,
      _pageSize: Option[Int] = None,
      _queryId: Option[UUID] = None,
      _customReference: Option[String] = None
  ) extends CustomRefsReqBase {
    def queryId: Option[UUID] = _queryId
    def customReference: Option[String] = _customReference
  }

  private val queryParamsFormat = Json.format[QueryParams]

  // Generating dummy object to collect keys from it in runtime
  val systemKeys =
    queryParamsFormat.writes(QueryParams(Some(1), Some(2), Some(UUID.randomUUID()), Some(""))).value.keySet

  implicit val queryExecReads: Reads[FilterParams ~ QueryParams] = {

    val filterParamsReads = {

      Reads
        .map[literal.Literal]
        .flatMap(
          params =>
            params.keysIterator
              .filterNot(systemKeys.contains)
              .find(_.startsWith("_"))
              .fold[Reads[FilterParams]](Reads.pure(params))(p =>
                Reads(_ => JsError(__ \ p, s"param cannot start with '_'"))))
    }

    pairReads(filterParamsReads, queryParamsFormat)
  }

  def buildQueryReq(savedQuery: SavedQuery, page: Option[Int], pageSize: Option[Int]): Either[String, QueryReq] =
    controllers.validatePageParams(page, pageSize).map {
      case Some((_page, _pageSize)) =>
        savedQuery.query.copy(limit = Some(_pageSize), offset = Some((_page - 1) * _pageSize))
      case None =>
        savedQuery.query
    }
}

class SavedQueryExecController(components: ControllerComponents,
                               action: PantheonActionBuilder,
                               queryHelper: QueryHelper,
                               auth: Authorization,
                               sqRepo: SavedQueryRepo,
                               schemaRepo: SchemaRepo,
                               connectionProvider: BackendAwareConnectionProvider)(
    implicit dbConf: DatabaseConfig[JdbcProfile]
) extends PantheonBaseController(components)
    with ContextLogging {

  import controllers.SavedQueryExecController._

  def execute(catalogId: UUID, savedQueryId: UUID) =
    action(parse.json[FilterParams ~ QueryParams])(
      request =>
        implicit ctx => {

          auth
            .checkPermission(request.user, ResourceType.SavedQuery, ActionType.Read, Some(catalogId), savedQueryId)
            .flatMap(
              ifAuthorized {

                val filterParams ~ (queryParams @ QueryParams(_page, _pageSize, _, _)) = request.body
                val refs = queryParams.customRefs

                logCustomRefs(refs)

                (for {
                  sq <- EitherT[Future, Result, SavedQuery](
                    sqRepo
                      .find(catalogId, savedQueryId)
                      .map(_.toRight(
                        NotFound(ActionError(s"""Saved query "$savedQueryId" not found in catalog: $catalogId"""))))
                  )
                  query <- EitherT.fromEither[Future](
                    buildQueryReq(sq, _page, _pageSize).leftMap(e => BadRequest(ActionError(e)))
                  )
                  res <- connectionProvider.withConnection(catalogId, sq.schemaId)(
                    queryHelper.executeQuery(
                      _,
                      catalogId,
                      sq.schemaId,
                      query,
                      refs.queryId,
                      refs.customReference,
                      filterParams
                    )
                  )
                } yield res).fold(identity, Ok(_))
              }
            )
      }
    )
}
