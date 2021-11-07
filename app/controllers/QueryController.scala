package controllers

import java.util.UUID

import play.api.mvc._
import play.api.libs.json.{Json, _}
import pantheon.schema._
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import pantheon.{Connection, TopN, _}
import pantheon.util.Logging.ContextLogging
import Writables.jsonWritable
import config.PantheonActionBuilder
import services.{Authorization, SchemaRepo}
import controllers.QueryController._
import QueryHelper.{Response, SqlColumnMetadata, valueLiteralWrites}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import services.serializers.QuerySerializers.{CustomRefsReqBase, PantheonQueryReq, QueryReqWithRefs}
import _root_.util.JsonSerializers.{pairReads, writesEnsuringFields}
import _root_.util.JsonSerializers.enumEntryWrites
import pantheon.util.BoolOps
import cats.instances.future._
import controllers.AuthorizationControllerCommons.ifAuthorized
import services.Authorization.{ActionType, ResourceType}

import scala.concurrent.{ExecutionContext, Future}
object QueryController {

  case class CompatibilityQueryRequest(
      query: PantheonQueryReq,
      constraints: Option[List[String]],
      queryId: Option[UUID] = None,
      customReference: Option[String] = None
  ) extends CustomRefsReqBase

  // Flat compatibility response model
  case class AttributeResponse(ref: String,
                               schema: Option[String],
                               dimension: String,
                               hierarchy: String,
                               level: String,
                               attribute: String,
                               metadata: ValueMap)

  case class MeasureResponse(ref: String,
                             schema: Option[String],
                             measure: String,
                             aggregate: Option[MeasureAggregate],
                             metadata: ValueMap)

  case class CompatibilityResponse(measures: Seq[MeasureResponse], dimensions: Seq[AttributeResponse])

  implicit val sortOrderReads = Reads.enumNameReads(SortOrder)
  implicit val orderedColumnReads = Json.reads[pantheon.OrderedColumn]
  implicit val topNReads = Json.reads[TopN]
  implicit val compatQueryReads = Json.reads[CompatibilityQueryRequest]
  implicit val compatibilityAttrWrites = Json.writes[AttributeResponse]

  implicit val compatibilityMeasureWrites =
    writesEnsuringFields(
      // Discuss: intentionally omitting optinoal field 'schema' here. Maybe we should be consistently follow our serialization conventions?
      Json.writes[MeasureResponse],
      "aggregate"
    )

  implicit val compatibilityWrites = Json.writes[CompatibilityResponse]

  // had to unnest these classes due to https://bugs.openjdk.java.net/browse/JDK-8057919

  case class NestedSchemaScopedMeasuresResponse(schema: Option[String], children: Seq[NestedMeasureResponse])

  case class NestedMeasureResponse(
      ref: String,
      measure: String,
      aggregate: Option[MeasureAggregate],
      metadata: ValueMap
  )

  case class NestedSchemaScopedAttributesResponse(schema: Option[String], children: Seq[NestedDimensionResponse])

  case class NestedDimensionResponse(dimension: String, children: Seq[NestedHierarchyResponse])

  case class NestedHierarchyResponse(hierarchy: String, children: Seq[NestedLevelResponse])

  case class NestedLevelResponse(level: String, children: Seq[NestedAttributeResponse])

  case class NestedAttributeResponse(attribute: String, metadata: ValueMap, ref: String)

  case class NestedCompatibilityResponse(
      measures: Seq[NestedSchemaScopedMeasuresResponse],
      dimensions: Seq[NestedSchemaScopedAttributesResponse]
  )

  private implicit val nestedMeasureResponseWrites = Json.writes[NestedMeasureResponse]
  private implicit val nestedSchemaScopedMeasuresWrites = Json.writes[NestedSchemaScopedMeasuresResponse]
  private implicit val nestedAttributeResponseWrites = Json.writes[NestedAttributeResponse]
  private implicit val nestedLevelResponseWrites = Json.writes[NestedLevelResponse]
  private implicit val nestedHierarchyResponseWrites = Json.writes[NestedHierarchyResponse]
  private implicit val nestedDimensionResponseWrites = Json.writes[NestedDimensionResponse]
  private implicit val nestedSchemaScopedAttributesWrites = Json.writes[NestedSchemaScopedAttributesResponse]
  implicit val nestedCompatibilityResponseWrites = Json.writes[NestedCompatibilityResponse]
}

class QueryController(controllerComponents: ControllerComponents,
                      action: PantheonActionBuilder,
                      queryHelper: QueryHelper,
                      auth: Authorization,
                      schemaRepo: SchemaRepo,
                      connectionProvider: BackendAwareConnectionProvider)(implicit dbConf: DatabaseConfig[JdbcProfile])
    extends PantheonBaseController(controllerComponents)
    with ContextLogging {

  def execute(catalogId: UUID, schemaId: UUID) =
    action(parse.json[QueryReqWithRefs]) { request => implicit ctx =>
      auth
        .checkPermission(request.user, ResourceType.Schema, ActionType.Read, Some(catalogId), schemaId)
        .flatMap(ifAuthorized {

          val refs = request.body.customRefs

          logCustomRefs(refs)

          connectionProvider
            .withConnection(catalogId, schemaId)(
              queryHelper
                .executeQuery(
                  _,
                  catalogId,
                  schemaId,
                  request.body,
                  refs.queryId,
                  refs.customReference
                )
            )
            .fold(identity, Ok(_))
        })
    }

  private def compatibilityAction(catalogId: UUID, schemaId: UUID)(
      represent: CompatibilityResponse => JsValue
  ) =
    action(parse.json[CompatibilityQueryRequest]) { request => implicit ctx =>
      auth
        .checkPermission(request.user, ResourceType.Schema, ActionType.Read, Some(catalogId), schemaId)
        .flatMap(
          ifAuthorized {

            def getCompatibility(conn: Connection,
                                 q: PantheonQuery,
                                 constraints: List[String]): CompatibilityResponse = {

              val compat = conn.schema.getCompatibility(q, constraints)

              val compatMeasures = compat.measures.map { m =>
                val agg = m.measure match {
                  case m: AggMeasure        => Some(m.aggregate)
                  case m: FilteredMeasure   => Some(m.base.aggregate)
                  case m: CalculatedMeasure => None
                }
                MeasureResponse(m.reference, m.importedFromSchema, m.name, agg, m.measure.metadata)
              }(collection.breakOut)

              val compatDimensions = compat.dimensionAttributes.map { d =>
                AttributeResponse(
                  d.reference,
                  d.importedFromSchema,
                  d.dimensionName,
                  d.hierarchyName,
                  d.levelName,
                  d.attribute.name,
                  d.attribute.metadata
                )
              }(collection.breakOut)

              CompatibilityResponse(compatMeasures, compatDimensions)
            }

            val query = request.body.query.buildPantheonQuery
            val constraints = request.body.constraints
            logCustomRefs(request.body.customRefs)

            connectionProvider
              .withConnection(catalogId, schemaId)(c =>
                Future.successful(Right(Ok(represent(getCompatibility(c, query, constraints.getOrElse(Nil)))))))
              .merge

          }
        )
    }

  def compatibility(catalogId: UUID, schemaId: UUID) =
    compatibilityAction(catalogId, schemaId)(Json.toJson(_))

  def nestedCompatibility(catalogId: UUID, schemaId: UUID) =
    compatibilityAction(catalogId, schemaId) { compatResp =>
      def nestMeasures(
          measures: Seq[MeasureResponse]
      ): Seq[NestedSchemaScopedMeasuresResponse] = {
        def mesToNested(c: MeasureResponse) =
          NestedMeasureResponse(c.ref, c.measure, c.aggregate, c.metadata)

        measures
          .groupBy(_.schema)
          .flatMap {
            case (schema, measureResps) =>
              List(
                NestedSchemaScopedMeasuresResponse(
                  schema,
                  measureResps.map(mesToNested)
                )
              )
          }(collection.breakOut)
      }

      def nestDimensions(attrs: Seq[AttributeResponse]): Seq[NestedSchemaScopedAttributesResponse] = {

        def attrsToNested(c: Seq[AttributeResponse]): Seq[NestedDimensionResponse] =
          c.groupBy(_.dimension)
            .map {
              case (dim, attrResps) =>
                NestedDimensionResponse(
                  dim,
                  attrResps
                    .groupBy(_.hierarchy)
                    .map {
                      case (h, attrResps) =>
                        NestedHierarchyResponse(
                          h,
                          attrResps
                            .groupBy(_.level)
                            .map {
                              case (l, attrResps) =>
                                NestedLevelResponse(
                                  l,
                                  attrResps.map(r => NestedAttributeResponse(r.attribute, r.metadata, r.ref))
                                )
                            }(collection.breakOut)
                        )
                    }(collection.breakOut)
                )
            }(collection.breakOut)

        attrs
          .groupBy(_.schema)
          .flatMap {
            case (schema, attrResps) =>
              Seq(NestedSchemaScopedAttributesResponse(schema, attrsToNested(attrResps)))
          }(collection.breakOut)
      }

      Json.toJson(NestedCompatibilityResponse(nestMeasures(compatResp.measures), nestDimensions(compatResp.dimensions)))
    }
}
