package pantheon

import java.io.PrintWriter
import java.net.URI
import java.sql
import java.sql.{Driver, DriverManager, ResultSet, SQLFeatureNotSupportedException}
import java.util.Properties
import java.util.concurrent.ScheduledExecutorService
import java.util.logging.Logger

import cats.instances.either._
import cats.syntax.either._
import cats.syntax.traverse._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.ws.rs.NotSupportedException
import pantheon.util.{DriverRegistry, withResource}

import scala.collection.JavaConverters._
import cats.instances.option._
import pantheon.util.Tap
import scala.collection.immutable.Map
import java.net.URLEncoder

import cats.data.{EitherT, State}
import cats.data.EitherT.{fromEither, liftF}
import pantheon.DataSource.DriverClassLoc
import pantheon.util.internal.dsprops
import pantheon.util.internal.dsprops.{optional => _, _}
import pantheon.util.internal.urlBuilder._
import JdbcDataSource.{DriverClassName, DriverClassPath}
import pantheon.backend.calcite.CalciteConnection

object DataSource {

  type DsName = String
  type User = String
  type Password = Option[Int]

  case class DriverClassLoc(name: String, path: Option[String]) {
    def getDriver(): Driver = DriverRegistry.get(name, path)
    def toProps: Map[String, String] = Map(DriverClassName -> name) ++ path.map(DriverClassPath -> _)
  }

  private def urlEncode: String => String = URLEncoder.encode(_, "UTF8")

  private def addJdbcCredentials(props: Props, user: String, password: Option[String]) =
    props + ("user" -> user) ++ password.map("password" -> _)

  // syncing schema-carrying field with props
  private def syncSchemaParamWith(fld: Option[String], props: Props): (Props, Option[String]) =
    (props ++ fld.map(CalciteConnection.SchemaKey -> _)) -> fld.orElse(props.get(CalciteConnection.SchemaKey))

  private def optional(str: String): EitherT[PropsState, String, Option[String]] = liftF(dsprops.optional(str))

  // http://hsqldb.org/doc/guide/dbproperties-chapt.html
  def createHsqlFoodmart(name: String,
                         props: Map[String, String] = Map.empty,
                         classLocation: Option[DriverClassLoc] = None,
                         hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val url = s"jdbc:hsqldb:res:foodmart"
    val (_props, _) =
      syncSchemaParamWith(Some("foodmart"), addJdbcCredentials(props, "FOODMART", Some("FOODMART")))
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  // https://jdbc.postgresql.org/documentation/80/connect.html
  def createPostgres(name: String,
                     host: Option[String],
                     port: Option[Int],
                     user: String,
                     password: Option[String],
                     dbName: String,
                     schema: Option[String] = None,
                     props: Map[String, String] = Map.empty,
                     classLocation: Option[DriverClassLoc] = None,
                     hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _schema) =
      syncSchemaParamWith(
        schema.filterNot(_.isEmpty).orElse(Some("public")),
        addJdbcCredentials(props, user, password)
      )
    val url = host match {
      case None => s"jdbc:postgresql:$dbName"
      case Some(_host) =>
        s"jdbc:postgresql://${_host}"
          .addOptInt(":", port)
          .+(s"/$dbName")
          .addParams("?", "&", "currentSchema" -> _schema)
    }
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  // https://github.com/yandex/clickhouse-jdbc
  def createClickhouse(name: String,
                       host: String,
                       port: Option[Int],
                       user: Option[String],
                       password: Option[String],
                       dbName: Option[String],
                       props: Map[String, String] = Map.empty,
                       classLocation: Option[DriverClassLoc] = None,
                       hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _dbName) = syncSchemaParamWith(dbName, user.fold(props)(addJdbcCredentials(props, _, password)))
    val url = s"jdbc:clickhouse://$host".addOptInt(":", port).addOpt("/", _dbName)
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  object MySqlProtocol extends Enumeration {
    val mysql, loadbalance, replication, mysqlx = Value
  }

  // TODO: add multihost support later if needed
  // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html
  def createMySql(name: String,
                  host: String,
                  port: Option[Int],
                  user: String,
                  password: Option[String],
                  dbName: Option[String],
                  protocol: Option[MySqlProtocol.Value],
                  props: Map[String, String] = Map.empty,
                  classLocation: Option[DriverClassLoc] = None,
                  hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _dbName) = syncSchemaParamWith(dbName, addJdbcCredentials(props, user, password))
    val url = {
      val proto = protocol.getOrElse(MySqlProtocol.mysql)
      val protocolPart =
        if (proto == MySqlProtocol.mysqlx) proto.toString
        else s"jdbc:" + proto.toString
      //the following characters [/, :, @, (, ), [, ], &, #, =, ?, 'space']  must be url encoded if used in parts of the url
      s"$protocolPart://${urlEncode(host)}".addOptInt(":", port).addOpt("/", _dbName.map(urlEncode))
    }

    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  // TODO: support failover hosts later if needed
  // https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.03/en-US/ff15928cf5594d78b841fbbe649f04b4.html
  def createHana(name: String,
                 host: String,
                 port: Int,
                 user: String,
                 password: Option[String],
                 dbName: Option[String],
                 schema: Option[String] = None,
                 props: Map[String, String] = Map.empty,
                 classLocation: Option[DriverClassLoc] = None,
                 hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _schema) =
      syncSchemaParamWith(
        schema.filterNot(_.isEmpty).orElse(Some("PUBLIC")),
        addJdbcCredentials(props, user, password)
      )
    val url = s"jdbc:sap://$host:$port".addParams("/?", "&", "databaseName" -> dbName, "currentschema" -> _schema)
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  //https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.java.doc/src/tpc/imjcc_r0052342.html
  def createDB2(name: String,
                host: String,
                port: Option[Int],
                user: String,
                password: Option[String],
                dbName: String,
                props: Map[String, String] = Map.empty,
                classLocation: Option[DriverClassLoc] = None,
                hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _) = syncSchemaParamWith(Some(user.toUpperCase), addJdbcCredentials(props, user, password))
    val url = s"jdbc:db2://$host".addOptInt(":", port) + s"/$dbName"
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  // https://docs.oracle.com/cd/B28359_01/java.111/b31224/urls.htm
  def createOracle(name: String,
                   host: String,
                   port: Option[Int],
                   user: String,
                   password: Option[String],
                   dbName: String,
                   props: Map[String, String] = Map.empty,
                   classLocation: Option[DriverClassLoc] = None,
                   hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _) = syncSchemaParamWith(Some(user.toUpperCase), addJdbcCredentials(props, user, password))
    val url = s"jdbc:oracle:thin:@//$host".addOptInt(":", port) + s"/$dbName"
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  // https://developer.teradata.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ
  def createTeradata(name: String,
                     host: String,
                     port: Option[Int],
                     user: String,
                     password: Option[String],
                     dbName: String,
                     props: Map[String, String] = Map.empty,
                     classLocation: Option[DriverClassLoc] = None,
                     hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): JdbcDataSource = {
    val (_props, _) = syncSchemaParamWith(Some(user.toUpperCase), addJdbcCredentials(props, user, password))
    val url = s"jdbc:teradata://$host/DATABASE=$dbName".addOptInt(",DBS_PORT=", port)
    JdbcDataSource(name, url, _props, classLocation, hikariHousekeeperThreadPool)
  }

  def createMongo(name: String, host: String, database: String) = MongoSource(name, host, database)

  def create(name: String,
             // TODO: Enum
             typ: String,
             properties: Map[String, String],
             hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): Either[String, DataSource] = {

    def pool = hikariHousekeeperThreadPool

    def build(v: EitherT[PropsState, String, DataSource]) = v.value.runA(properties).value

    def buildJdbc(
        b: EitherT[PropsState, String, (Props, Option[DriverClassLoc]) => DataSource]): Either[String, DataSource] =
      build(
        for {
          buildDs <- b
          classLoc <- readDriverClassLoc
          props <- liftF[PropsState, String, Props](State.get)
        } yield buildDs(props, classLoc)
      )

    (typ match {

      case "hsqldb-foodmart" =>
        buildJdbc(EitherT.pure((props, classLoc) => createHsqlFoodmart(name, props, classLoc, pool)))

      case "postgresql" =>
        buildJdbc(for {
          dbName <- mandatory(dataBaseKey)
          host <- optional(hostKey)
          port <- readPortOpt
          user <- mandatory(userKey)
          password <- optional(passwordKey)
          schema <- optional(schemaKey)
        } yield createPostgres(name, host, port, user, password, dbName, schema, _, _, pool))

      case "clickhouse" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPortOpt
            dbName <- optional(dataBaseKey)
            user <- optional(userKey)
            password <- optional(passwordKey)
          } yield createClickhouse(name, host, port, user, password, dbName, _, _, pool)
        )

      case "mysql" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPortOpt
            dbName <- optional(dataBaseKey)
            user <- mandatory(userKey)
            password <- optional(passwordKey)
            protocolRaw <- optional(protocolKey)
            protocol <- fromEither[PropsState](protocolRaw.traverse(parseMysqlProtocol))
          } yield createMySql(name, host, port, user, password, dbName, protocol, _, _, pool)
        )

      case "hana" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPort
            dbName <- optional(dataBaseKey)
            user <- mandatory(userKey)
            password <- optional(passwordKey)
            schema <- optional(schemaKey)
          } yield createHana(name, host, port, user, password, dbName, schema, _, _, pool)
        )

      case "db2" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPortOpt
            dbName <- mandatory(dataBaseKey)
            user <- mandatory(userKey)
            password <- optional(passwordKey)
            schema <- optional(schemaKey)
          } yield createDB2(name, host, port, user, password, dbName, _, _, pool)
        )

      case "oracle" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPortOpt
            dbName <- mandatory(dataBaseKey)
            user <- mandatory(userKey)
            password <- optional(passwordKey)
          } yield createOracle(name, host, port, user, password, dbName, _, _, pool)
        )

      case "teradata" =>
        buildJdbc(
          for {
            host <- mandatory(hostKey)
            port <- readPortOpt
            dbName <- mandatory(dataBaseKey)
            user <- mandatory(userKey)
            password <- optional(passwordKey)
          } yield createTeradata(name, host, port, user, password, dbName, _, _, pool)
        )

      case "jdbc" =>
        buildJdbc(
          mandatory("url").map(url => JdbcDataSource(name, url, _, _, pool))
        )

      case "reflective" =>
        build(mandatory("target").map(ReflectiveDataSource(name, _)))

      case "druid" =>
        def parseBoolean(s: String): Either[String, Boolean] = s.trim match {
          case "true"  => Right(true)
          case "false" => Right(false)
          case s       => Left(s"cannot parse boolean from $s")
        }

        build(
          for {
            url <- mandatory(urlKey)
            coordinationUrl <- mandatory("coordinatorUrl")
            approxDecimal <- optional("approxDecimal").subflatMap(_.traverse(parseBoolean))
            approxDistinctCount <- optional("approxDistinctCount").subflatMap(_.traverse(parseBoolean))
            approxTopN <- optional("approxTopN").subflatMap(_.traverse(parseBoolean))
          } yield
            DruidSource(
              name,
              new URI(url),
              new URI(coordinationUrl),
              approxDecimal.getOrElse(false),
              approxDistinctCount.getOrElse(false),
              approxTopN.getOrElse(false)
            )
        )

      case "mongo" =>
        build(
          for {
            host <- mandatory(hostKey)
            database <- mandatory(dataBaseKey)
          } yield createMongo(name, host, database)
        )

      case t => Left(s"type is not supported")

    }).leftMap(s"cannot build data source of type '$typ' from props, " + _)
  }

}
sealed abstract class DataSource extends AutoCloseable {
  def name: String
  def testConnection(): Boolean

  override def close(): Unit = ()
}

object JdbcDataSource {

  val DriverClassName = "driverClassName"
  val DriverClassPath = "driverClassPath"
  val HikariCpPrefix = "hikaricp."
  val HikariConfigFileProp = "hikaricp.configurationFile"

  if (System.getProperty(HikariConfigFileProp) == null)
    System.setProperty(HikariConfigFileProp, "/hikari.properties")

  class DriverDataSource(driver: Driver, jdbcUrl: String, props: Properties) extends javax.sql.DataSource {
    override def getConnection: sql.Connection = driver.connect(jdbcUrl, props)

    override def getConnection(username: String, password: String): sql.Connection = getConnection

    override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException()

    override def isWrapperFor(iface: Class[_]): Boolean = false

    override def getLogWriter: PrintWriter = throw new SQLFeatureNotSupportedException()

    override def setLogWriter(out: PrintWriter): Unit = throw new SQLFeatureNotSupportedException()

    override def setLoginTimeout(seconds: Int): Unit = DriverManager.setLoginTimeout(seconds)

    override def getLoginTimeout: Int = DriverManager.getLoginTimeout

    override def getParentLogger: Logger = driver.getParentLogger
  }
}

/**
  * Data source which defines parameters to access JDBC
  * @param name
  * @param properties    Can contain properties from https://commons.apache.org/proper/commons-dbcp/configuration.html
  *                      to configure underlying BasicDataSource
  *                      as well as other properties to be used by different backend technologies
  *                      (for example, CalciteConnection uses 'schema' and 'catalog' properties)
  * @param classLocation Used to load drivers dynamically.

  */
// TODO: dont expose hikariHousekeeperThreadPool parameter. Need discuss JdbcDataSource in general.
case class JdbcDataSource(override val name: String,
                          url: String,
                          properties: Map[String, String],
                          classLocation: Option[DriverClassLoc],
                          hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None)
    extends DataSource {

  import JdbcDataSource._

  def poolingDataSource: javax.sql.DataSource =
    createdDs.getOrElse {
      synchronized {
        createdDs.getOrElse {
          val ds = createDataSource()
          createdDs = Some(ds)
          ds
        }
      }
    }

  def withConnection[T](block: java.sql.Connection => T) = withResource(poolingDataSource.getConnection())(block(_))

  override def testConnection(): Boolean = withConnection(_.isValid(10))

  override def close(): Unit = synchronized {
    createdDs.foreach { ds =>
      ds.close()
      createdDs = None
    }
  }

  @volatile
  private var createdDs: Option[HikariDataSource] = None

  private def createDataSource(): HikariDataSource = {

    val (hikariMap, dsMap) = properties.partition { case (k, _) => k.startsWith(HikariCpPrefix) }

    val hikariProps = new Properties()
      .tap(
        _.putAll(
          hikariMap.map { case (k, v) => k.stripPrefix(HikariCpPrefix) -> v }.asJava
        )
      )

    val dsProps = new Properties().tap(_.putAll(dsMap.asJava))

    val config = new HikariConfig(hikariProps)
      .tap { conf =>
        hikariHousekeeperThreadPool.foreach(conf.setScheduledExecutor)

        classLocation.map(_.getDriver()) match {
          case Some(d) => conf.setDataSource(new DriverDataSource(d, url, dsProps))
          case None =>
            conf.setJdbcUrl(url)
            conf.setDataSourceProperties(dsProps)
        }
      }

    new HikariDataSource(config)
  }

  //cannot return ResultSet directly because it requires open connection to operate
  def executeSqlQuery[R](query: String)(materialize: ResultSet => R): R =
    concurrent.blocking {
      withConnection { conn =>
        withResource(conn.createStatement().executeQuery(query))(materialize)
      }
    }
}

case class DruidSource(override val name: String,
                       url: URI,
                       coordinatorUrl: URI,
                       approxDecimal: Boolean,
                       approxDistinctCount: Boolean,
                       approxTopN: Boolean)
    extends DataSource {

  override def testConnection(): Boolean = {
    throw new NotSupportedException("testConnection() not supported for DruidSource")
  }

}

case class MongoSource(override val name: String, host: String, database: String) extends DataSource {
  override def testConnection(): Boolean = ??? // TODO
}

case class ReflectiveDataSource(override val name: String, target: Any) extends DataSource {
  override def testConnection(): Boolean = {
    true
  }
}
