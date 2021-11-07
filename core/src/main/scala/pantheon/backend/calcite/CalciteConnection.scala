package pantheon.backend.calcite

import java.sql._
import java.util.Properties
import java.util.function.Consumer
import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.druid.{DruidQuery, DruidSchema}
import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.adapter.jdbc.{JdbcConvention, JdbcSchema}
import org.apache.calcite.adapter.mongodb.MongoSchemaFactory
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.config.{CalciteConnectionProperty, NullCollation}
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan._
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.{RelNode, RelRoot}
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema.{SchemaPlus, Schemas}
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.SqlShuttle
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools._
import org.apache.calcite.util.Holder
import pantheon.backend.calcite.support.dialect.{BigQueryDialect, ClickHouseDialect, Db2Dialect, HanaDialect, OracleDialect, TeradataDialect}
import pantheon.backend.{BackendConnection, BackendStatement}
import pantheon.planner.QueryPlan
import pantheon.schema.{Column, Schema, Table}
import pantheon.util.Logging.LoggingContext
import pantheon._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class CalcitePreparedStatement(val physicalPlan: CalcitePlan, val preparedStatement: PreparedStatement) {
  def execute(): ResultSet = preparedStatement.executeQuery()
}

object CalciteConnection {

  val SchemaKey = "schema"
  val CatalogKey = "catalog"

  private val dialectFactory = {

    val defaultFactory = new SqlDialectFactoryImpl()

    new SqlDialectFactory {
      override def create(databaseMetaData: DatabaseMetaData): SqlDialect = {

        val dbName: String =
          Try(databaseMetaData.getDatabaseProductName()) match {
            case Success(s) => s.toUpperCase
            case Failure(t) => throw new RuntimeException("while detecting database product name", t)
          }

        // TODO: remove this, use DEFAULT instead
        val ctx: SqlDialect.Context =
          SqlDialect.EMPTY_CONTEXT
            .withDatabaseProductName(dbName)
            .withNullCollation(NullCollation.LOW)

        dbName match {
          case "CLICKHOUSE"      => ClickHouseDialect.DEFAULT
          case "GOOGLE BIGQUERY" => new BigQueryDialect(ctx)
          case "HDB"             => HanaDialect.DEFAULT
          case "ORACLE"          => OracleDialect.DEFAULT
          case "TERADATA"        => TeradataDialect.DEFAULT

          case name if name.startsWith("DB2") => Db2Dialect.DEFAULT

          case _ => defaultFactory.create(databaseMetaData)
        }
      }
    }
  }

  def apply(dataSources: Seq[DataSource], schema: Option[Schema] = None, params: Map[String, String] = Map.empty) = {

    val connectionProperties = {
      val properties = new Properties()
      properties.setProperty(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName, "true")

      // properties set by default can be overridden
      properties.putAll(params.asJava)
      properties
    }

    val connection =
      DriverManager
        .getConnection("jdbc:calcite:", connectionProperties)
        .unwrap(classOf[org.apache.calcite.jdbc.CalciteConnection])

    def rootSchema = connection.getRootSchema

    def createJdbcSchema(ds: JdbcDataSource): JdbcSchema = {
      val expression = Schemas.subSchemaExpression(rootSchema, ds.name, classOf[JdbcSchema])
      val javaDs = ds.poolingDataSource
      val dialect = JdbcSchema.createDialect(dialectFactory, javaDs)
      val convention = new JdbcConvention(dialect, expression, ds.name)
      new JdbcSchema(javaDs,
                     dialect,
                     convention,
                     ds.properties.getOrElse(CatalogKey, null),
                     ds.properties.getOrElse(CalciteConnection.SchemaKey, null))
    }

    def addDataSource(dataSource: DataSource): SchemaPlus = {
      val dsSchema = dataSource match {
        case ds: JdbcDataSource =>
          createJdbcSchema(ds)

        case DruidSource(_, url, coordinatorUrl, _, _, _) =>
          new DruidSchema(url.toString, coordinatorUrl.toString, true)

        case MongoSource(_, host, database) =>
          new MongoSchemaFactory()
            .create(null, dataSource.name, Map[String, AnyRef]("host" -> host, "database" -> database).asJava)

        case ReflectiveDataSource(_, target) =>
          new ReflectiveSchema(target)
      }
      rootSchema.add(dataSource.name, dsSchema)
    }

    dataSources.foreach(addDataSource)
    new CalciteConnection(dataSources, connection, schema)
  }
}

class CalciteConnection(val dataSources: Seq[DataSource],
                        val connection: org.apache.calcite.jdbc.CalciteConnection,
                        schema: Option[Schema] = None)
    extends BackendConnection {

  lazy val frameworkConfig: FrameworkConfig = {
    Frameworks.newConfigBuilder
      .defaultSchema(rootSchema)
      .parserConfig(SqlParser.configBuilder(SqlParser.Config.DEFAULT).setUnquotedCasing(Casing.UNCHANGED).build())
      .programs(Programs.standard)
      .context(Contexts.chain(Contexts.of(connection.config()), Contexts.of(tableScanFactory)))
      .build
  }

  lazy val (cluster, calciteSchema) = CalciteUtil.getClusterAndSchema(frameworkConfig)

  lazy val sqlParser: CalciteSqlParser = new CalciteSqlParser(frameworkConfig, cluster)

  def relBuilder: RelBuilder = new RelBuilder(frameworkConfig.getContext, cluster, calciteSchema) {}

  def tableScanFactory: RelFactories.TableScanFactory =
    (toRelContext, table) => table.toRel(ViewExpanders.simpleContext(toRelContext.getCluster))

  def rootSchema = connection.getRootSchema

  override def close(implicit ctx: LoggingContext): Unit = connection.close()

  def sqlToRel(sql: String): RelRoot = sqlParser.queryToRel(sql)

  def sqlProjectToRel(sql: String): RelNode = {
    val rel = sqlToRel(sql).rel.asInstanceOf[LogicalProject]
    val input = rel.getInput

    // The following logic is required for Druid in case of complex metrics
    // as complex metrics are not passed through with '*'
    // Here we adding missing fields from underlying DruidQuery by hand
    if (input.isInstanceOf[DruidQuery]) {
      val originalField = rel.getRowType.getFieldNames.asScala
      val additionalFields =
        input.getRowType.getFieldNames.asScala.zipWithIndex.filterNot { case (f, i) => originalField.exists(_ == f) }

      if (additionalFields.nonEmpty) {
        val projects = ImmutableList.builder().addAll(rel.getProjects)
        val fieldNames = ImmutableList.builder().addAll(rel.getRowType.getFieldNames)
        additionalFields.foreach {
          case (f, i) =>
            projects.add(RexInputRef.of(i, input.getRowType))
            fieldNames.add(f)
        }
        LogicalProject.create(input, projects.build(), fieldNames.build())
      } else rel
    } else rel
  }

  def tableToRel(table: Table): RelNode =
    sqlProjectToRel(tableToSql(table))

  // TODO: Move these methods out of connection
  def getLogicalPlan(rel: RelRoot): RelNode = rel.project()
  // TODO: Look if its possible to get physical plan without creating a prepared statement.
  def getPhysicalPlan(logicalPlan: RelNode): RelNode = prepare(logicalPlan).physicalPlan.rel

  def executeQuery(sql: String): JdbcRowSet = new JdbcRowSet(connection.createStatement().executeQuery(sql))

  def prepare(rel: RelNode): CalcitePreparedStatement = {
    val runner = connection.unwrap(classOf[RelRunner])
    var physicalPlan: CalcitePlan = null
    val c = Hook.PROGRAM.addThread(new Consumer[Holder[Program]] {
      override def accept(h: Holder[Program]) = {
        h.set((planner, rel, requiredOutputTraits, materializations, lattices) => {
          val result = frameworkConfig.getPrograms.asScala.foldLeft(rel)(
            (rel, p) => p.run(planner, rel, requiredOutputTraits, materializations, lattices)
          )
          physicalPlan = new CalcitePlan(result)
          result
        })
      }
    })
    val stmt = runner.prepare(rel)
    new CalcitePreparedStatement(physicalPlan, stmt)
  }

  def execute(rel: RelNode): ResultSet = prepare(rel).execute()

  override def createStatement(plan: QueryPlan)(implicit ctx: LoggingContext): BackendStatement = {
    new CalciteStatement(this, createLogicalPlan(plan))
  }

  override def createSqlStatement(query: String)(implicit ctx: LoggingContext): BackendStatement = {
    val parsedQuery = sqlParser.parseQuery(query)
    val q = if (schema.nonEmpty) substituteViews(parsedQuery) else parsedQuery
    new CalciteStatement(this, new CalcitePlan(sqlParser.rel(q).project()))
  }

  private def createLogicalPlan(plan: QueryPlan)(implicit ctx: LoggingContext): CalcitePlan =
    ctx.withSpan("CalciteConnection#createLogicalPlan") { _ =>
      new CalciteGenerator(this).generate(plan)
    }

  private def substituteViews(query: SqlNode): SqlNode = {
    query.accept(
      new ViewExpander(schema.map(_.name), name => schema.fold[Option[SqlNode]](None)(_.getTable(name).map(parseView))))
  }

  private def parseView(table: Table): SqlNode =
    sqlParser.parseQuery(tableToSql(table))

  private def tableToSql(table: Table): String = {
    val from = table.definition.fold(
      pTable => s""""${table.dataSource.name}"."${pTable.value}"""",
      sql => s"(${sql.value})"
    )

    def columnToSql(c: Column) =
      c.expression
        .map(e => s"""($e) as "${c.name}"""")
        .getOrElse(s""""${c.name}"""")

    val cols = {
      if (table.addAllDBColumns) table.columns.withFilter(_.expression.isDefined).map(columnToSql) :+ "*"
      else table.columns.map(columnToSql)
    }.mkString(", ")

    s"select $cols from $from"
  }

  class ViewExpander(schemaName: Option[String], views: String => Option[SqlNode]) extends SqlShuttle {
    private def stripSchemaName(id: SqlIdentifier): Unit = {
      if (id.names.size() > 1 && schemaName.contains(id.names.get(0))) {
        id.names = id.names.subList(1, id.names.size())
      }
    }

    private def convertTableRef(node: SqlNode): SqlNode = {
      if (node == null) node
      else {
        node.getKind match {
          case SqlKind.IDENTIFIER =>
            val id = node.asInstanceOf[SqlIdentifier]
            stripSchemaName(id)
            val name = id.names.asScala.mkString(".")
            views(name)
              .map(v => SqlValidatorUtil.addAlias(v, name))
              .getOrElse(throw new RuntimeException(s"Table '$name' not found"))
          case SqlKind.AS =>
            val call = node.asInstanceOf[SqlCall]
            val op: SqlNode = call.operand(0)
            if (op.getKind == SqlKind.IDENTIFIER) {
              val id = op.asInstanceOf[SqlIdentifier]
              stripSchemaName(id)
              val name = id.names.asScala.mkString(".")
              call.setOperand(0, views(name).getOrElse(throw new RuntimeException(s"Table '$name' not found")))
            }
            call
          case _ =>
            node
        }
      }
    }

    override def visit(call: SqlCall): SqlNode = {
      val c = super.visit(call)
      c.getKind match {
        case SqlKind.SELECT =>
          val node = c.asInstanceOf[SqlSelect]
          node.setFrom(convertTableRef(node.getFrom))
          node

        case SqlKind.JOIN =>
          val node = c.asInstanceOf[SqlJoin]
          node.setLeft(convertTableRef(node.getLeft))
          node.setRight(convertTableRef(node.getRight))
          node

        case _ =>
          c
      }
    }
  }

  override def verifyTable(t: Table)(implicit ctx: LoggingContext): Future[List[String]] = {

    // does not run secondary check if primary has failed
    def runChecks(primary: => List[String], secondary: => Option[String]): List[String] = {
      val p = primary
      if (p.isEmpty) secondary.toList
      else p
    }

    def columnExprCheck(columns: Seq[Column], from: String): Option[String] =
      columns.flatMap(_.expression) match {
        case Nil => None
        case exprs =>
          try {
            getPhysicalPlan(sqlToRel(s"""select ${exprs.mkString(",")} from $from""").rel)
            None
          } catch {
            // TODO: be more precise on exception type
            case NonFatal(e) => Some(s"Some of the following expressions ${exprs} caused an error: ${e.getMessage}")
          }
      }

    def verifyPhysicalTable(pTableName: String, dataSource: DataSource, columns: List[Column]): List[String] = {

      def tableAndColumnRefCheck: List[String] = {

        val calciteTable = rootSchema.getSubSchema(dataSource.name).getTable(pTableName)

        if (calciteTable == null) List(s"Table not found")
        else {
          val containedInDb = calciteTable.getRowType(new JavaTypeFactoryImpl()).getFieldNames.asScala.toSet
          val missingColumns = columns.filterNot(c => containedInDb(c.name) || c.expression.isDefined)
          missingColumns.map(c => s"column '${c.name}' is missing")(collection.breakOut)
        }
      }

      runChecks(tableAndColumnRefCheck, columnExprCheck(columns, s""""${dataSource.name}"."$pTableName""""))
    }

    def verifySqlTable(sql: String, dataSource: DataSource, columns: List[Column]): List[String] = {

      def sqlAndColumnRefCheck: List[String] =
        try {
          val containedInQuery = getPhysicalPlan(sqlToRel(sql).rel).getRowType.getFieldNames.asScala.toSet
          val missingColumns = columns.filterNot(c => containedInQuery(c.name) || c.expression.isDefined)
          missingColumns.map(c => s"column '${c.name}' is missing")(collection.breakOut)
        } catch {
          case NonFatal(e) =>
            List(s"Error during query execution: ${e.getMessage}")
        }

      runChecks(sqlAndColumnRefCheck, columnExprCheck(columns, "(" + sql + ")"))
    }

    Future.successful(
      t.definition
        .fold(
          ptable => verifyPhysicalTable(ptable.value, t.dataSource, t.columns),
          sql => verifySqlTable(sql.value, t.dataSource, t.columns)
        )
    )
  }
}
