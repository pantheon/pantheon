package controllers

import java.util.UUID

import pantheon.backend.calcite.CalciteBackend
import pantheon.schema.{Schema, Table}
import play.api.mvc.{AbstractController, ControllerComponents}
import services.{Authorization, SchemaRepo}
import play.api.libs.json.Json
import play.api.libs.json.Json.obj
import Writables.jsonWritable
import SchemaRepo.{SchemaId, showKey}
import cats.data.NonEmptyList
import config.PantheonActionBuilder
import controllers.AuthorizationControllerCommons.ifAuthorized
import controllers.SchemaVerificationController.TableDefinitionError
import pantheon.backend.BackendConnection
import services.Authorization.{ActionType, ResourceType}
import services.CatalogRepo.CatalogId
import pantheon.util.withResourceManual
import util.JsonSerializers.nelFormat
import cats.syntax.traverse._
import cats.instances.future._
import cats.instances.list._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SchemaVerificationController {

  case class TableDefinitionError(name: String, definition: String, dataSource: String, errors: NonEmptyList[String])

  implicit val tableErrWrites = Json.writes[TableDefinitionError]
}

class SchemaVerificationController(components: ControllerComponents,
                                   action: PantheonActionBuilder,
                                   findAndCompile: (CatalogId, SchemaId) => Future[Option[Schema]],
                                   auth: Authorization)
    extends AbstractController(components) {

  //Hardcoding Calcite backend here for now
  def verify(catalogId: UUID, schemaId: UUID) =
    action { req => implicit ctx =>
      {
        def verifyTables(c: BackendConnection, tables: List[Table]): Future[List[TableDefinitionError]] = {
          def mkTableDefError(t: Table, errs: NonEmptyList[String]): TableDefinitionError =
            TableDefinitionError(
              t.name,
              t.definition.fold(name => s"physical table: '${name.value}'", sql => sql.value),
              t.dataSource.name,
              errs
            )
          tables.flatTraverse(t => c.verifyTable(t).map(NonEmptyList.fromList(_).map(mkTableDefError(t, _)).toList))
        }

        auth
          .checkPermission(req.user, ResourceType.Schema, ActionType.Read, Some(catalogId), schemaId)
          .flatMap(
            ifAuthorized {
              findAndCompile(catalogId, schemaId).flatMap {
                _.fold(Future.successful(NotFound(showKey(catalogId, schemaId))))(
                  s => {
                    withResourceManual(CalciteBackend().getConnection(s))(_.close)(
                      verifyTables(_, s.tables)
                        .map(tableErrors =>
                          if (tableErrors.isEmpty) Ok
                          else UnprocessableEntity(obj("tables" -> tableErrors)))
                    )
                  }
                )
              }
            }
          )
      }
    }
}
