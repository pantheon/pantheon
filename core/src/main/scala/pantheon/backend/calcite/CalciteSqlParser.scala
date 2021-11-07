package pantheon.backend.calcite

import java.util
import java.util.Properties

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlConformance, SqlConformanceEnum, SqlValidatorImpl}
import org.apache.calcite.sql.{SqlInsert, SqlNode}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter}
import org.apache.calcite.tools.FrameworkConfig

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class CalciteSqlParser(config: FrameworkConfig, cluster: RelOptCluster) {

  private val operatorTable = config.getOperatorTable
  private val parserConfig = config.getParserConfig
  private val sqlToRelConverterConfig = config.getSqlToRelConverterConfig
  private val convertletTable = config.getConvertletTable
  private val defaultSchema = config.getDefaultSchema

  private val javaTypeFactory = cluster.getTypeFactory.asInstanceOf[JavaTypeFactory]

  private val planner = {
    val planner = cluster.getPlanner
    planner.setExecutor(config.getExecutor())
    val traitDefs = config.getTraitDefs
    if (traitDefs != null) {
      planner.clearRelTraitDefs()
      for (d <- traitDefs.asScala) planner.addRelTraitDef(d)
    }
    planner
  }

  def parseStatement(sql: String): SqlNode = parser(sql).parseStmt()

  def parseQuery(sql: String): SqlNode = parser(sql).parseQuery()

  def validate(sqlNode: SqlNode): SqlNode = validator().validate(sqlNode)

  def queryToRel(sql: String): RelRoot = rel(parseQuery(sql))

  def rel(sql: SqlNode, top: Boolean = true, catalogReader: CalciteCatalogReader = createCatalogReader()): RelRoot = {
    val cluster = RelOptCluster.create(planner, createRexBuilder())
    val config = SqlToRelConverter.configBuilder
      .withConfig(sqlToRelConverterConfig)
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .build
    val sqlToRelConverter =
      new SqlToRelConverter(new ViewExpanderImpl,
                            validator(catalogReader),
                            catalogReader,
                            cluster,
                            convertletTable,
                            config)
    val root = sqlToRelConverter.convertQuery(sql, true, top)
    val relBuilder = config.getRelBuilderFactory.create(cluster, null)
    root
      .withRel(RelDecorrelator.decorrelateQuery(sqlToRelConverter.flattenTypes(root.rel, true), relBuilder))
  }

  private def parser(sql: String) = SqlParser.create(sql, parserConfig)

  private def validator(catalogReader: CalciteCatalogReader = createCatalogReader()): SqlValidatorImpl =
    new SqlValidatorImpl(operatorTable, catalogReader, javaTypeFactory, conformance) {
      setIdentifierExpansion(true)

      override protected def getLogicalSourceRowType(sourceRowType: RelDataType, insert: SqlInsert): RelDataType =
        javaTypeFactory.toSql(super.getLogicalSourceRowType(sourceRowType, insert))

      override protected def getLogicalTargetRowType(targetRowType: RelDataType, insert: SqlInsert): RelDataType =
        javaTypeFactory.toSql(super.getLogicalTargetRowType(targetRowType, insert))
    }

  private def conformance: SqlConformance = {
    val context = config.getContext
    if (context != null) {
      val connectionConfig = context.unwrap(classOf[CalciteConnectionConfig])
      if (connectionConfig != null) return connectionConfig.conformance
    }
    SqlConformanceEnum.DEFAULT
  }

  private def createCatalogReader() = {
    val rootSchema = CalciteUtil.rootSchema(defaultSchema)
    val context = config.getContext
    val connectionConfig =
      if (context != null) context.unwrap(classOf[CalciteConnectionConfig])
      else {
        val properties = new Properties
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName,
                               String.valueOf(parserConfig.caseSensitive))
        new CalciteConnectionConfigImpl(properties)
      }
    new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                             CalciteSchema.from(defaultSchema).path(null),
                             javaTypeFactory,
                             connectionConfig)
  }

  private def createRexBuilder() = new RexBuilder(javaTypeFactory)

  class ViewExpanderImpl extends RelOptTable.ViewExpander {
    override def expandView(rowType: RelDataType,
                            queryString: String,
                            schemaPath: util.List[String],
                            viewPath: util.List[String]): RelRoot =
      rel(parseQuery(queryString), false, createCatalogReader().withSchemaPath(schemaPath))
  }

}
