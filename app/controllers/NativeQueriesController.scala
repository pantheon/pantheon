package controllers

import java.util.UUID

import pantheon.backend.calcite.JdbcRowSet
import pantheon.JdbcDataSource
import play.api.mvc.ControllerComponents
import services.{Authorization, DataSourceRepo}
import Writables.jsonWritable
import config.PantheonActionBuilder
import management.{Native, QueryDescription}
import pantheon.util.Logging.{ContextLogging, LoggingContext}
import pantheon.util.withResource
import controllers.NativeQueriesController._
import play.api.libs.json.Json
import NativeQueriesController.nativeQueryReads
import controllers.AuthorizationControllerCommons.ifAuthorized
import services.Authorization.{ActionType, ResourceType}
import services.serializers.QuerySerializers.CustomRefsReqBase

import scala.concurrent.Future
import scala.util.Failure

object NativeQueriesController {

  case class NativeQueryRequest(
      query: String,
      queryId: Option[UUID] = None,
      customReference: Option[String] = None
  ) extends CustomRefsReqBase

  implicit val nativeQueryReads = Json.reads[NativeQueryRequest]
}

class NativeQueriesController(
    controllerComponents: ControllerComponents,
    action: PantheonActionBuilder,
    auth: Authorization,
    dsr: DataSourceRepo,
    queryHelper: QueryHelper
) extends PantheonBaseController(controllerComponents)
    with ContextLogging {
  import QueryHelper._

  def nativeQuery(catId: UUID, dsId: UUID) = action(parse.json[NativeQueryRequest]) { req => implicit ctx =>
    auth
      .checkPermission(req.user, ResourceType.DataSource, ActionType.Read, Some(catId), dsId)
      .flatMap(
        ifAuthorized {

          val query = req.body.query
          val refs = req.body.customRefs

          logCustomRefs(refs)

          dsr
            .getDataSource(catId, dsId)
            .flatMap {
              case None =>
                Future.successful(
                  NotFound(ActionError(s"Data source $dsId not found in catalog: $catId"))
                )
              case Some(ds: JdbcDataSource) =>
                queryHelper
                  .execRegistered(
                    refs.queryId,
                    QueryDescription(Native(query, dsId), catId, refs.customReference),
                    plans = None,
                    Failure(new NotImplementedError("Native query cancellation is not implemented"))
                  )(
                    ds.executeSqlQuery(query) { resultSet =>
                      val rs = new JdbcRowSet(resultSet)
                      Response(
                        rs.fields.map(fld => SqlColumnMetadata(fld.name, primitiveType(fld.dataType))),
                        rs.rows.map(getRow(rs.fields, _)).toList,
                        Nil,
                        Nil
                      )
                    }
                  )
                  .map(_.fold(identity, Ok(_)))

              case Some(ds) =>
                Future.successful(
                  NotImplemented(ActionError(
                    s"DataSource $dsId of type '${ds.getClass.getSimpleName}' is not supported. Only JDBC datasources are supported"))
                )
            }
        }
      )
  }
}
