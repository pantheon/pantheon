package pantheon.backend.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import pantheon.{DataSource, JdbcDataSource}
import pantheon.backend.{BackendConnection, BackendStatement}
import pantheon.planner.QueryPlan
import pantheon.schema.{Column, Schema, Table}
import pantheon.util.Logging.{ContextLogging, LoggingContext}

import scala.concurrent.Future

class SparkConnection(schema: Schema, sparkSession: SparkSession) extends BackendConnection with ContextLogging {

  val dataSources = schema.tables.map(_.dataSource)

  val tables: Map[String, DataFrame] = schema.tables.map {
    case Table(name, definition, dataSource, columns, _) =>
      val sqlString = definition.fold(
        ptable => "\"" + ptable.value + "\"",
        sql => s"(${sql.value})"
      )

      val dataFrame = createDataFrame(sqlString, getDataSource(dataSource.name))

      val exprs = columns.collect {
        case Column(colName, Some(expression), _, _, _) =>
          s"($expression) as `$colName`"
      }

      name -> (if (exprs.nonEmpty) dataFrame.selectExpr("*" :: exprs: _*) else dataFrame)
  }(collection.breakOut)

  // guard against creating views several times for sql statement purposes
  private lazy val ensureViewsCreated: Unit = createViews()

  def createViews(): Unit = {
    tables.foreach { case (name, df) => df.createTempView(s"`$name`") }
  }

  def getDataSource(name: String): DataSource =
    dataSources.find(_.name == name).getOrElse(throw new Exception(s"No data source was found: $name"))

  def createDataFrame(sql: String, ds: DataSource): DataFrame = ds match {
    case JdbcDataSource(name, url, properties, _, _) =>
      val props = new Properties()
      for ((k, v) <- properties) props.setProperty(k, v)
      sparkSession.read.jdbc(url, sql, props)
    case _ =>
      throw new Exception(s"Data source type not supported: ${ds.getClass}")
  }

  override def createSqlStatement(query: String)(implicit ctx: LoggingContext): BackendStatement = {
    ensureViewsCreated
    new SparkStatement(sparkSession.sql(query), sparkSession.sparkContext)
  }

  override def createStatement(plan: QueryPlan)(implicit ctx: LoggingContext): BackendStatement =
    new SparkStatement(new SparkGenerator(tables).generate(plan), sparkSession.sparkContext)

  override def close(implicit ctx: LoggingContext): Unit = sparkSession.close()

  // TBD: Implement
  override def verifyTable(table: Table)(implicit ctx: LoggingContext): Future[List[String]] = ???
}
