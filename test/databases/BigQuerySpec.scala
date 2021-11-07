package databases

import java.net.URI
import java.time.ZoneId

import org.scalatest.{MustMatchers, WordSpec}
import pantheon.backend.calcite.{CalciteBackend}
import pantheon.util.{Logging, RowSetUtil}
import pantheon._
import pantheon.backend.BackendPlan
import pantheon.planner.ASTBuilders._
import pantheon.util.Tap
import pantheon.util.Misc.hikariHousekeeperThreadPool
class BigQuerySpec extends WordSpec with MustMatchers {

  val ds: DataSource = JdbcDataSource(
    "foodmart",
    "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:355;" +
      "ProjectId=contiamo.com:contiamo;" +
      "OAuthType=2;" +
      "OAuthAccessToken=ya29.GluFBcOH1RL2tkgyLZ349g4_J-3-qACCbvSHKsWXyOQPfZVB4hxe9DLSlIfvHJpxQCHkoQXE73Iead-eb3oiUzKEB_G4pEESzinh26rmXLX_lBneRaumZFsBkj3Y;" +
      "OAuthClientId=977385342095.apps.googleusercontent.com;" +
      "OAuthRefreshToken=--------;" +
      "OAuthClientSecret=--------;",
    Map(
      "schema" -> "test"
    ),
    None,
    Some(hikariHousekeeperThreadPool)
  )

  implicit def loggingContext = Logging.newContext

  val schemaPsl =
    """schema x {
      |dimension Dim {
      |    attribute timestampWithZone(column = "simpleWithDates.timestamp_field_18")
      |    attribute dbl(column = "simpleWithDates.double_field_24")
      |  }
      |
      |  measure num(column = "simpleWithDates.int64_field_0")
      |
      |table simpleWithDates  (dataSource="foodmart"){
      | column int64_field_0
      | column double_field_24
      | column timestamp_field_18
      |}
      |
      |}""".stripMargin

  def test[T]: (Connection => T) => T =
    Pantheon.withConnection[T](new InMemoryCatalog(Map("_test_" -> schemaPsl), List(ds)), "_test_", CalciteBackend())(_)

  def testAggregate(query: PantheonQuery): (BackendPlan, List[String]) = test { conn =>
    val stmt = conn.createStatement(query).tap(_.mustBe('right))
    stmt.backendPhysicalPlan -> RowSetUtil.toStringList(stmt.execute(), zoneId = Some(ZoneId.of("UTC")))
  }

  def testSql(q: String): (BackendPlan, List[String]) = test { conn =>
    val stmt = conn.createStatement(SqlQuery(q)).tap(_.mustBe('right))
    stmt.backendPhysicalPlan -> RowSetUtil.toStringList(stmt.execute(), zoneId = Some(ZoneId.of("UTC")))
  }

  "big query must work in basic scenarios" ignore {

    // Warning: Date conversions do not work
    // cast(timestamp_field_17 as TIMESTAMP ),
    // cast(timestamp_field_17 as TIMESTAMP_WITH_LOCAL_TIME_ZONE ),
    // Warning: can floor only Timestamp with time zone
    "sql" in {

      val (plan, resp) = testSql("""select
      cast(double_field_24  as tinyint) as a,
      cast(double_field_24 as smallint) as b,
      cast(double_field_24 as INTEGER) as c,
      cast(double_field_24 as BIGINT) as d,
      cast(double_field_24 as VARCHAR) as e,
      cast(int64_field_0 as FLOAT) as f,
      cast(int64_field_0 as DOUBLE) as g,
      cast(bool_field_1 as BOOLEAN) as h,
      timestamp_field_18 as j,
      floor(timestamp_field_18 to day) as flrd
      FROM "foodmart"."simpleWithDates"
      limit 2
      """)

      plan.toString mustBe
        """JdbcToEnumerableConverter
            |  JdbcProject(a=[CAST($24):TINYINT], b=[CAST($24):SMALLINT], c=[CAST($24):INTEGER], d=[CAST($24):BIGINT], e=[CAST($24):VARCHAR CHARACTER SET "UTF-16LE" COLLATE "UTF-16LE$en_US$primary"], f=[CAST($0):FLOAT], g=[CAST($0):DOUBLE], h=[CAST($1):BOOLEAN], j=[$18], flrd=[FLOOR($18, FLAG(DAY))])
            |    JdbcSort(fetch=[2])
            |      JdbcTableScan(table=[[foodmart, simpleWithDates]])
            |""".stripMargin

      resp mustBe List(
        "a=5; b=5; c=5; d=5; e=5; f=7.0; g=7.0; h=true; j=2016-08-09 00:05:26; flrd=2016-08-09 00:00:00",
        "a=5; b=5; c=5; d=5; e=5; f=1.0; g=1.0; h=true; j=2016-11-12 00:53:11; flrd=2016-11-12 00:00:00"
      )
    }

    "Aggregate" in {
      val (plan, resp) =
        testAggregate(
          AggregateQuery(rows = List("Dim.timestampWithZone"),
                    measures = List("num"),
                    filter = Some(ref("Dim.dbl") > lit(0)),
                    limit = Some(5)))

      plan.toString mustBe
        """JdbcToEnumerableConverter
            |  JdbcSort(fetch=[5])
            |    JdbcAggregate(group=[{18}], num=[SUM($0)])
            |      JdbcFilter(condition=[>($24, 0)])
            |        JdbcTableScan(table=[[foodmart, simpleWithDates]])
            |""".stripMargin

      resp mustBe List(
        "Dim.timestampWithZone=2016-08-09 00:05:26; num=7",
        "Dim.timestampWithZone=2016-11-12 00:53:11; num=1",
        "Dim.timestampWithZone=2017-06-15 00:00:44; num=5",
        "Dim.timestampWithZone=2017-02-04 00:00:12; num=4",
        "Dim.timestampWithZone=2017-05-18 00:40:17; num=2"
      )
    }
  }

}
