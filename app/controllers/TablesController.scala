package controllers

import java.util.UUID

import config.PantheonActionBuilder
import play.api.mvc.ControllerComponents
import services.Authorization.{ActionType, ResourceType}
import services.{Authorization, DataSourceRepo, SchemaRepo}
import controllers.TablesController._
import AuthorizationControllerCommons.ifAuthorized
import play.api.libs.json.{Json, OWrites}
import Writables._
import pantheon.backend.calcite.JdbcRowSet.JdbcRowIterator
import pantheon._
import pantheon.backend.calcite.{CalciteConnection, JdbcRowSet}
import pantheon.schema.Table
import services.show
import pantheon.util.Logging.LoggingContext
import pantheon.util.toGroupedMap

import scala.collection.immutable

object TablesController {
  case class TableResponse(name: String, columns: Seq[String])

  implicit val dsContentsRespWrites: OWrites[TableResponse] = Json.writes[TableResponse]
}

class TablesController(components: ControllerComponents,
                       action: PantheonActionBuilder,
                       auth: Authorization,
                       schemaRepo: SchemaRepo,
                       dsRepo: DataSourceRepo)
    extends PantheonBaseController(components) {

  def getTablesFromSchema(catId: UUID, schemaId: UUID) = {
    action.apply(
      r =>
        implicit ctx =>
          auth
            .checkPermission(r.user, ResourceType.Schema, ActionType.Read, Some(catId), schemaId)
            .flatMap(
              ifAuthorized {
                schemaRepo.findAndCompile(catId, schemaId).map {
                  case None => NotFound(SchemaRepo.showKey(catId, schemaId))
                  case Some(schema) =>
                    Ok(
                      schema.tables.map(
                        t =>
                          TableResponse(
                            t.name,
                            columns = {
                              val tableColumns = t.columns.map(_.name)
                              if (t.addAllDBColumns) (tableColumns ++ getDbColumns(t)).distinct
                              else tableColumns
                            }
                        ))
                    )
                }
              }
        )
    )
  }

  def getTablesFromDataSource(catId: UUID, dsId: UUID) =
    action.apply(
      r =>
        _ =>
          auth
            .checkPermission(r.user, ResourceType.Catalog, ActionType.Read, Some(catId), dsId)
            .flatMap(
              ifAuthorized(
                dsRepo
                  .getDataSource(catId, dsId)
                  .map {
                    case Some(ds: JdbcDataSource) => Ok(getTablesFromJdbcDb(ds, None))
                    case Some(ds @ (_: DruidSource | _: ReflectiveDataSource | _: MongoSource)) =>
                      NotImplemented(
                        s"DataSource ${ds.name} of type '${ds.getClass.getSimpleName}' is not supported. " +
                          s"Only JDBC datasources are supported"
                      )
                    case None => NotFound(DataSourceRepo.showKey(catId, dsId))
                  }
              )))

  private def getTablesFromJdbcDb(ds: JdbcDataSource,
                                  tableNamePattern: Option[String]): immutable.Iterable[TableResponse] = {

    def getTableAndColumn(row: Row): (String, String) = {
      val tableNameIndex = 2
      val columnNameIndex = 3
      row.getString(tableNameIndex) -> row.getString(columnNameIndex)
    }

    val tableWithColumns: Map[String, List[String]] =
      ds.withConnection { conn =>
        val metadataResultSet = conn.getMetaData
          .getColumns(
            null,
            ds.properties.getOrElse(CalciteConnection.SchemaKey, null),
            tableNamePattern.orNull,
            null
          )

        toGroupedMap(new JdbcRowIterator(metadataResultSet).map(getTableAndColumn).toList)(identity)
      }

    tableWithColumns.map(Function.tupled(TableResponse))
  }

  private def getDbColumns(t: Table)(implicit ctx: LoggingContext): Iterable[String] = {
    t.dataSource match {
      case ds: JdbcDataSource =>
        t.definition.fold(
          physicalTableName => {
            val tableNamePattern = physicalTableName.value
            val tables = getTablesFromJdbcDb(ds, Some(tableNamePattern))
            // if this situation happens we need to decide how to handle it
            assert(
              tables.size <= 1,
              s"""unexpected situation: database responded with more than one table
                  | table name pattern: '$tableNamePattern'
                  | tables: ${show(tables.map(_.toString))}"""
            )
            tables.flatMap(_.columns)
          },
          expression => ds.executeSqlQuery(expression.value)(new JdbcRowSet(_).fields.map(_.name))
        )
      case _: DruidSource | _: ReflectiveDataSource | _: MongoSource => Nil
    }
  }
}
