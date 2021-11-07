package jdbc

import java.lang.reflect.{Field, Modifier}
import java.sql.{DatabaseMetaData, DriverManager, PreparedStatement, ResultSet, SQLException, Types}
import java.util._
import java.util.concurrent.atomic.AtomicInteger
import java.{lang, util}

import com.google.common.base.Optional
import db.DatabaseCatalog
import org.apache.calcite.avatica.Meta._
import org.apache.calcite.avatica.MetaImpl._
import org.apache.calcite.avatica.QueryState.StateType
import org.apache.calcite.avatica.remote.TypedValue
import org.apache.calcite.avatica._
import org.apache.calcite.avatica.util.Unsafe
import pantheon.backend.calcite.{CalciteConnection, CalciteStatement}
import pantheon.backend.spark.SparkStatement
import pantheon.{Connection, SqlQuery}
import pantheon.util.{Logging, withResource}
import pantheon.util.Logging.ContextLogging
import services.{BackendConfigRepo, SchemaRepo}

import collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect._

class PantheonMetaImpl(
    backendConfigRepo: BackendConfigRepo,
    schemaRepo: SchemaRepo,
    connectionCacheConfig: Cache.Config,
    statementCacheConfig: Cache.Config
)(implicit ec: ExecutionContext)
    extends Meta
    with ContextLogging {

  case class ConnectionInfo(catalogId: UUID, connection: Connection)

  case class StatementInfo(preparedStatement: PreparedStatement) {
    lazy val resultSet = preparedStatement.executeQuery()

    def close(): Unit = {
      try {
        resultSet.close()
      } finally {
        preparedStatement.close()
      }
    }
  }

  private val calendar = Unsafe.localCalendar

  private val connectionCache: scala.collection.concurrent.Map[String, ConnectionInfo] =
    Cache.create(
      connectionCacheConfig, {
        case (k, ci) =>
          Option(ci).foreach {
            implicit val ctx = Logging.newContext
            logger.debug(s"Closing connection $k, while removing from cache")
            _.connection.close
          }
      }
    )

  private val statementIdGenerator = new AtomicInteger

  private val statementCache: scala.collection.concurrent.Map[Integer, StatementInfo] =
    Cache.create(
      statementCacheConfig, {
        case (k, si) =>
          Option(si).foreach {
            implicit val ctx = Logging.newContext
            logger.debug(s"Closing statement $k, while removing from cache")
            _.close()
          }
      }
    )

  //
  // Metadata helpers
  //

  protected def createResultSet[T <: AnyRef](connectionId: String, result: Seq[T] = Seq())(
      implicit context: ClassTag[T]): Meta.MetaResultSet = {
    val fields = getFields(classTag[T].runtimeClass)
    val frame = Frame.create(0, true, result.map(o => extractFields(fields, o)).asJava.asInstanceOf[util.List[AnyRef]])
    val columns = fieldMetaData(classTag[T].runtimeClass).columns
    createResultSet(connectionId,
                    Collections.emptyMap[String, AnyRef],
                    columns,
                    CursorFactory.create(Meta.Style.ARRAY, null, null),
                    frame)
  }

  protected def createResultSet(connectionId: String,
                                internalParameters: util.Map[String, AnyRef],
                                columns: util.List[ColumnMetaData],
                                cursorFactory: Meta.CursorFactory,
                                firstFrame: Meta.Frame): Meta.MetaResultSet =
    try {
      val signature = new Meta.Signature(columns,
                                         "",
                                         Collections.emptyList[AvaticaParameter],
                                         internalParameters,
                                         cursorFactory,
                                         Meta.StatementType.SELECT)
      MetaResultSet.create(connectionId, statementIdGenerator.getAndIncrement(), true, signature, firstFrame)
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    }

  protected def fieldMetaData(clazz: Class[_]): ColumnMetaData.StructType = {
    val metadata = getFields(clazz).toSeq.zipWithIndex.map {
      case (field, i) =>
        MetaImpl.columnMetaData(AvaticaUtils.camelToUpper(field.getName), i, field.getType, getColumnNullability(field))
    }
    ColumnMetaData.struct(metadata.asJava)
  }

  protected def extractFields(fields: Array[Field], obj: AnyRef): Array[AnyRef] = fields.map(_.get(obj))

  protected def getFields(clazz: Class[_]): Array[Field] =
    clazz.getFields.filter(field => Modifier.isPublic(field.getModifiers) && !Modifier.isStatic(field.getModifiers))

  protected def getColumnNullability(field: Field): Int = { // Check annotations first
    if (field.isAnnotationPresent(classOf[MetaImpl.ColumnNoNulls])) return DatabaseMetaData.columnNoNulls
    if (field.isAnnotationPresent(classOf[MetaImpl.ColumnNullable])) return DatabaseMetaData.columnNullable
    if (field.isAnnotationPresent(classOf[MetaImpl.ColumnNullableUnknown]))
      return DatabaseMetaData.columnNullableUnknown
    // check the field type to decide if annotated, as a fallback
    if (field.getType.isPrimitive) return DatabaseMetaData.columnNoNulls
    DatabaseMetaData.columnNullable
  }

  //
  // Overridden methods from interface
  //

  override def getDatabaseProperties(ch: Meta.ConnectionHandle): util.Map[Meta.DatabaseProperty, AnyRef] = {
    val conn = getConnection(ch.id)
    conn.backendConnection match {
      case c: CalciteConnection =>
        val driver = DriverManager.getDriver("jdbc:calcite:").asInstanceOf[org.apache.calcite.jdbc.Driver]
        val calciteMeta = driver.createMeta(c.connection.unwrap(classOf[AvaticaConnection]))
        calciteMeta.getDatabaseProperties(ch)

      case _ =>
        throw new UnsupportedOperationException
    }
  }

  // returns all tables in schema specified in connection
  override def getTables(ch: Meta.ConnectionHandle,
                         catalog: String,
                         schemaPattern: Meta.Pat,
                         tableNamePattern: Meta.Pat,
                         typeList: util.List[String]): MetaResultSet = {
    val conn = getConnection(ch.id)
    def tables =
      conn.schema.tables.map(t => new MetaTable("", conn.schemaName, t.name, "TABLE"))
    createResultSet(ch.id, tables)
  }

  // returns columns for specified table (table name shall be specified exactly without pattern matching)
  // requires table to be in the current schema
  override def getColumns(ch: Meta.ConnectionHandle,
                          catalog: String,
                          schemaPattern: Meta.Pat,
                          tableNamePattern: Meta.Pat,
                          columnNamePattern: Meta.Pat): MetaResultSet = {
    implicit val ctx = Logging.newContext
    val conn = getConnection(ch.id)
    val tableName = tableNamePattern.s
    conn.schema.getTable(tableName) match {
      case None =>
        createResultSet[MetaColumn](ch.id)
      case Some(table) =>
        val s = conn.createStatement(SqlQuery(s"select * from $tableName"))
        s.backendStatement match {
          case cs: CalciteStatement =>
            val metadata = cs.preparedStatement.preparedStatement.getMetaData
            val columns = for {
              i <- 1 to metadata.getColumnCount
            } yield
              new MetaColumn(
                "",
                conn.schemaName,
                tableName,
                metadata.getColumnName(i),
                metadata.getColumnType(i),
                metadata.getColumnTypeName(i),
                metadata.getPrecision(i),
                metadata.getScale(i),
                10,
                metadata.isNullable(i),
                metadata.getColumnDisplaySize(i),
                i,
                metadata.isNullable(i) match {
                  case DatabaseMetaData.columnNullable => "YES"
                  case DatabaseMetaData.columnNoNulls  => "NO"
                  case _                               => ""
                }
              )
            createResultSet(ch.id, columns)

          case _: SparkStatement =>
            throw new UnsupportedOperationException
        }
    }
  }

  override def getSchemas(ch: Meta.ConnectionHandle, catalog: String, schemaPattern: Meta.Pat): MetaResultSet = {
    val conn = getConnection(ch.id)
    createResultSet(ch.id, Seq(new MetaSchema("", conn.schemaName)))
  }

  override def getCatalogs(ch: Meta.ConnectionHandle): MetaResultSet =
    createResultSet[MetaCatalog](ch.id)

  override def getTableTypes(ch: Meta.ConnectionHandle): MetaResultSet =
    createResultSet(ch.id, Seq(new MetaTableType("TABLE")))

  override def getProcedures(ch: Meta.ConnectionHandle,
                             catalog: String,
                             schemaPattern: Meta.Pat,
                             procedureNamePattern: Meta.Pat): MetaResultSet =
    createResultSet[MetaProcedure](ch.id)

  override def getProcedureColumns(ch: Meta.ConnectionHandle,
                                   catalog: String,
                                   schemaPattern: Meta.Pat,
                                   procedureNamePattern: Meta.Pat,
                                   columnNamePattern: Meta.Pat): MetaResultSet =
    createResultSet[MetaProcedureColumn](ch.id)

  override def getColumnPrivileges(ch: Meta.ConnectionHandle,
                                   catalog: String,
                                   schema: String,
                                   table: String,
                                   columnNamePattern: Meta.Pat): MetaResultSet =
    createResultSet[MetaColumnPrivilege](ch.id)

  override def getTablePrivileges(ch: Meta.ConnectionHandle,
                                  catalog: String,
                                  schemaPattern: Meta.Pat,
                                  tableNamePattern: Meta.Pat): MetaResultSet =
    createResultSet[MetaTablePrivilege](ch.id)

  override def getBestRowIdentifier(ch: Meta.ConnectionHandle,
                                    catalog: String,
                                    schema: String,
                                    table: String,
                                    scope: Int,
                                    nullable: Boolean): MetaResultSet =
    createResultSet[MetaBestRowIdentifier](ch.id)

  override def getVersionColumns(ch: Meta.ConnectionHandle,
                                 catalog: String,
                                 schema: String,
                                 table: String): MetaResultSet =
    createResultSet[MetaVersionColumn](ch.id)

  override def getPrimaryKeys(ch: Meta.ConnectionHandle,
                              catalog: String,
                              schema: String,
                              table: String): MetaResultSet =
    createResultSet[MetaPrimaryKey](ch.id)

  // Implemented as in JdbcMeta
  override def getImportedKeys(ch: Meta.ConnectionHandle,
                               catalog: String,
                               schema: String,
                               table: String): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getExportedKeys(ch: Meta.ConnectionHandle,
                               catalog: String,
                               schema: String,
                               table: String): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getCrossReference(ch: Meta.ConnectionHandle,
                                 parentCatalog: String,
                                 parentSchema: String,
                                 parentTable: String,
                                 foreignCatalog: String,
                                 foreignSchema: String,
                                 foreignTable: String): MetaResultSet = null

  override def getTypeInfo(ch: Meta.ConnectionHandle): MetaResultSet = {
    val conn = getConnection(ch.id)
    conn.backendConnection match {
      case c: CalciteConnection =>
        withResource(c.connection.getMetaData.getTypeInfo) { rs =>
          JdbcResultSet.create(ch.id, statementIdGenerator.getAndIncrement(), rs)
        }

      case _ =>
        throw new UnsupportedOperationException
    }
  }

  // Implemented as in JdbcMeta
  override def getIndexInfo(ch: Meta.ConnectionHandle,
                            catalog: String,
                            schema: String,
                            table: String,
                            unique: Boolean,
                            approximate: Boolean): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getUDTs(ch: Meta.ConnectionHandle,
                       catalog: String,
                       schemaPattern: Meta.Pat,
                       typeNamePattern: Meta.Pat,
                       types: Array[Int]): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getSuperTypes(ch: Meta.ConnectionHandle,
                             catalog: String,
                             schemaPattern: Meta.Pat,
                             typeNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getSuperTables(ch: Meta.ConnectionHandle,
                              catalog: String,
                              schemaPattern: Meta.Pat,
                              tableNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getAttributes(ch: Meta.ConnectionHandle,
                             catalog: String,
                             schemaPattern: Meta.Pat,
                             typeNamePattern: Meta.Pat,
                             attributeNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getClientInfoProperties(ch: Meta.ConnectionHandle): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getFunctions(ch: Meta.ConnectionHandle,
                            catalog: String,
                            schemaPattern: Meta.Pat,
                            functionNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getFunctionColumns(ch: Meta.ConnectionHandle,
                                  catalog: String,
                                  schemaPattern: Meta.Pat,
                                  functionNamePattern: Meta.Pat,
                                  columnNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def getPseudoColumns(ch: Meta.ConnectionHandle,
                                catalog: String,
                                schemaPattern: Meta.Pat,
                                tableNamePattern: Meta.Pat,
                                columnNamePattern: Meta.Pat): MetaResultSet = null

  // Implemented as in JdbcMeta
  override def createIterable(stmt: Meta.StatementHandle,
                              state: QueryState,
                              signature: Signature,
                              parameters: util.List[TypedValue],
                              firstFrame: Meta.Frame): lang.Iterable[AnyRef] = null

  private def createPreparedStatement(connectionId: String, sql: String): PreparedStatement = {
    implicit val ctx = Logging.newContext

    val conn = getConnection(connectionId)
    val s = conn.createStatement(SqlQuery(sql))
    s.backendStatement match {
      case cs: CalciteStatement =>
        cs.preparedStatement.preparedStatement

      case _: SparkStatement =>
        throw new UnsupportedOperationException
    }
  }

  override def prepare(ch: Meta.ConnectionHandle, sql: String, maxRowCount: Long): Meta.StatementHandle = {
    val stmt = createPreparedStatement(ch.id, sql)
    if (maxRowCount > 0) stmt.setLargeMaxRows(maxRowCount)
    val id = statementIdGenerator.getAndIncrement()

    statementCache.put(id, StatementInfo(stmt))

    new Meta.StatementHandle(ch.id, id, JdbcResultSet.signature(stmt.getMetaData, null, sql, StatementType.SELECT))
  }

  override def prepareAndExecute(h: Meta.StatementHandle,
                                 sql: String,
                                 maxRowCount: Long,
                                 callback: Meta.PrepareCallback): ExecuteResult =
    prepareAndExecute(h, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount), callback)

  override def prepareAndExecute(h: Meta.StatementHandle,
                                 sql: String,
                                 maxRowCount: Long,
                                 maxRowsInFirstFrame: Int,
                                 callback: Meta.PrepareCallback): ExecuteResult = {
    val stmtInfo = StatementInfo(createPreparedStatement(h.connectionId, sql))
    if (maxRowCount > 0) stmtInfo.preparedStatement.setLargeMaxRows(maxRowCount)
    statementCache.put(h.id, stmtInfo)
    val sig = JdbcResultSet.signature(stmtInfo.resultSet.getMetaData, null, sql, StatementType.SELECT)
    new ExecuteResult(Seq[MetaResultSet](
      JdbcResultSet.create(h.connectionId, h.id, stmtInfo.resultSet, maxRowsInFirstFrame, sig)).asJava)
  }

  override def prepareAndExecuteBatch(h: Meta.StatementHandle,
                                      sqlCommands: util.List[String]): Meta.ExecuteBatchResult =
    throw new UnsupportedOperationException

  override def executeBatch(h: Meta.StatementHandle,
                            parameterValues: util.List[util.List[TypedValue]]): Meta.ExecuteBatchResult =
    throw new UnsupportedOperationException

  override def fetch(h: Meta.StatementHandle, offset: Long, fetchMaxRowCount: Int): Meta.Frame = {
    getConnection(h.connectionId)
    val stmt = statementCache.getOrElse(h.id, throw new MissingResultsException(h))
    JdbcResultSet.frame(null, stmt.resultSet, offset, fetchMaxRowCount, calendar, Optional.absent[Meta.Signature])
  }

  override def execute(h: Meta.StatementHandle,
                       parameterValues: util.List[TypedValue],
                       maxRowCount: Long): ExecuteResult =
    execute(h, parameterValues, AvaticaUtils.toSaturatedInt(maxRowCount))

  override def execute(h: Meta.StatementHandle,
                       parameterValues: util.List[TypedValue],
                       maxRowsInFirstFrame: Int): ExecuteResult = {
    getConnection(h.connectionId)
    val stmt = statementCache.getOrElse(h.id, throw new NoSuchStatementException(h))
    new ExecuteResult(
      Seq[MetaResultSet](JdbcResultSet.create(h.connectionId, h.id, stmt.resultSet, maxRowsInFirstFrame)).asJava)
  }

  override def createStatement(ch: Meta.ConnectionHandle): Meta.StatementHandle = {
    implicit val ctx = Logging.newContext
    logger.debug(s"createStatement(connectionId=${ch.id})")
    getConnection(ch.id) // to ensure that connection exists
    new Meta.StatementHandle(ch.id, statementIdGenerator.getAndIncrement(), null)
  }

  override def closeStatement(h: Meta.StatementHandle): Unit = {
    implicit val ctx = Logging.newContext
    logger.debug(s"closeStatement(statementId=${h.id}, connectionId=${h.connectionId})")
    statementCache.remove(h.id)
  }

  private def getConnectionInfo(id: String): ConnectionInfo = {
    if (id == null) throw new NullPointerException("Connection id is null.")
    connectionCache.getOrElse(id, throw new NoSuchConnectionException(s"Connection not found: $id"))
  }

  private def getConnection(id: String): Connection = {
    getConnectionInfo(id).connection
  }

  override def openConnection(ch: Meta.ConnectionHandle, info: util.Map[String, String]): Unit = {
    implicit val ctx = Logging.newContext
    logger.debug(s"openConnection(${ch.id})")
    val connInfo = createConnection(info)
    // closing just opened connection if connection with such id already exists
    connectionCache.putIfAbsent(ch.id, connInfo).foreach { _ =>
      logger.debug(s"Closing opened connection with existing id: ${ch.id}")
      connInfo.connection.close(ctx)
    }
  }

  private def createConnection(info: util.Map[String, String]): ConnectionInfo = {

    val catalogId =
      UUID.fromString(info.asScala.getOrElse("catalogId", throw new RuntimeException("'catalogId' is required")))
    val schemaName = info.asScala.getOrElse("schemaName", throw new RuntimeException("'schemaName' is required"))

    val connFuture = for {
      backend <- backendConfigRepo.getBackendForSchema(catalogId, schemaName)
      catalog = new DatabaseCatalog(catalogId, schemaRepo)
    } yield ConnectionInfo(catalogId, new Connection(catalog, schemaName, backend))

    Await.result(connFuture, 20.seconds)
  }

  override def closeConnection(ch: Meta.ConnectionHandle): Unit = {
    implicit val ctx = Logging.newContext
    logger.debug(s"closeConnection(connectionId=${ch.id})")
    connectionCache.remove(ch.id)
  }

  @tailrec
  private def advance(rs: ResultSet, offset: Long): Boolean = {
    if (offset == 0) true
    else if (!rs.next()) false
    else advance(rs, offset - 1)
  }

  override def syncResults(sh: Meta.StatementHandle, state: QueryState, offset: Long): Boolean = {
    // make sure connection exists
    getConnection(sh.connectionId)
    state.`type` match {
      case StateType.SQL =>
        val stmtInfo = StatementInfo(createPreparedStatement(sh.connectionId, state.sql))
        statementCache.put(sh.id, stmtInfo).foreach(_.close())
        advance(stmtInfo.resultSet, offset)
      case _ =>
        throw new UnsupportedOperationException
    }
  }

  override def commit(ch: Meta.ConnectionHandle): Unit = throw new UnsupportedOperationException

  override def rollback(ch: Meta.ConnectionHandle): Unit = throw new UnsupportedOperationException

  // TODO revisit impl
  override def connectionSync(ch: Meta.ConnectionHandle,
                              connProps: Meta.ConnectionProperties): Meta.ConnectionProperties = {
    new ConnectionPropertiesImpl(true, true, java.sql.Connection.TRANSACTION_NONE, null, "Foodmart")
  }
}
