package controllers

import java.util.{Properties, UUID}

import play.api.libs.json._
import play.api.test._
import play.api.test.Helpers._
import _root_.util.Fixtures
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.play.BaseOneAppPerSuite
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually, scaled}
import dao.Tables.SavedQueriesRow
import controllers.NativeQueriesController.NativeQueryRequest
import pantheon.{JdbcDataSource, OrderedColumn, SortOrder, TopN}
import pantheon.QueryType.Aggregate
import services.QueryHistoryRepo._
import services.CatalogRepo.CatalogId
import services.DataSourceRepo.DataSourceId
import services.SchemaRepo.SchemaId
import controllers.Writables.jsonWritable
import play.api.Application
import QueryHistoryIntegrationSpec._
import play.api.libs.functional.~
import _root_.util.JsonSerializers.{EnumEntryReadsProvider, pairWrites}
import java.sql.DriverManager

import controllers.QueryHistoryController.{NativeHistoryRecord, QueryHistoryListResponse, QueryHistoryRecord, SchemaBasedHistoryRecord}

import scala.concurrent.duration._
import Fixtures.pagedResponseReads
import controllers.helpers.CrudlActions.PagedResponse

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random
import play.api.mvc.Result
import management.QueryHistoryRecordType
import pantheon.schema.DsProviders
import pantheon.util.DriverRegistry
import services.serializers.QuerySerializers.QueryReq
import services.serializers.QuerySerializers.queryReqFormat

object QueryHistoryIntegrationSpec {

  implicit val queryHistoryRecordReads: Reads[QueryHistoryRecord] = {

    val eerp = new EnumEntryReadsProvider(QueryHistoryRecordType)
    import eerp.reads
    val sReads = Json.reads[SchemaBasedHistoryRecord]
    val nReads = Json.reads[NativeHistoryRecord]
    jv =>
      {
        (jv \ "type").validate[QueryHistoryRecordType].flatMap {
          case QueryHistoryRecordType.Native => nReads.reads(jv)
          case QueryHistoryRecordType.Schema => sReads.reads(jv)
        }
      }
  }

  def getRegisteredQueries(app: Application,
                           catId: UUID,
                           customRefPattern: Option[String] = None): Seq[QueryHistoryRecord] = {
    val params = customRefPattern.fold("")(p => s"?customRefPattern=$p")
    contentAsJson(route(app, FakeRequest(GET, s"/catalogs/$catId/queryHistory$params")).get)
      .as[PagedResponse[QueryHistoryRecord]]
      .data
  }
}

class QueryHistoryIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {


  // this method is used to terminate long running queries
  // it is commented because 'ALTER SESSION $id END STATEMENT' command is not supported in HSQL 2.3.1
  // HSQL 2.3.4 where it is supported has a problem with short default query timeouts which is not configurable via JDBC Statement.setQueryTimeout
//  def cleanSessions(): Unit = {
//
//    val execute: String => List[String] = {
//      query => {
//        val s = c.createStatement()
//        val r = RowSetUtil.toStringList(new JdbcRowSet(s.executeQuery(query)))
//        s.close()
//        r
//      }
//    }
//
//    def getSessionsAndStatements(): immutable.Seq[(Int, String)] = {
//      val sessionRx = "SESSION_ID=(\\d+);".r.unanchored
//      val statementRx = "(?s)CURRENT_STATEMENT=(.*?);".r.unanchored
//      execute("select * from information_schema.system_sessions")
//        .map { s =>
//          val sessionRx(session) = s
//          val statementRx(statemnt) = s
//          session.toInt -> statemnt
//        }
//    }
//
//    getSessionsAndStatements().foreach{ case(id, _) => execute(s"ALTER SESSION $id END STATEMENT")}
////    println(getSessionsAndStatements().mkString("\n"))
//  }

  def prepareTmpTable(): Unit = {
    val driver = DriverRegistry.get("org.hsqldb.jdbcDriver", Some("classpath:jdbc/hsqldb-foodmart/hsqldb-2.4.1.jar"))
    val props = new Properties()
    props.setProperty("user", "FOODMART")
    props.setProperty("password", "FOODMART")
    pantheon.util.withResource(driver.connect("jdbc:hsqldb:res:foodmart", props)) { hsqlConnection =>
      println("populating table TMP with dummy data")

      val s = hsqlConnection.createStatement()

      s.executeUpdate("""CREATE TABLE TMP (ID INT NOT NULL, TITLE VARCHAR(50) NOT NULL);""".stripMargin)

      import scala.concurrent.ExecutionContext.Implicits.global
      def x(start: Int) = {
        (start to start + 100000).foreach(i =>
          s.addBatch(s"""INSERT INTO TMP (ID, TITLE) VALUES ($i, '${Random.nextInt()}');"""))
        s.executeBatch()
      }

      import scala.concurrent.duration._
      Await.result(Future(x(0)), 5.minutes)
      s.close
      println("done populating table TMP")
    }
  }

//  TODO: uncomment when after switching to HSQL 2.3.4+ (version where session termination is supported)
//  def dropTmpTable() = {
//    val s = c.createStatement()
//    s.executeUpdate("drop table TMP;")
//    s.close
//  }

//  override def afterEach(): Unit = {
//    cleanSessions()
//    dropTmpTable()
//    super.afterEach()
//  }

  implicit val sortOrderWrites = Writes.enumNameWrites[SortOrder.type]
  implicit val orderedColumnWrites = Json.writes[OrderedColumn]
  implicit val topNWrites = Json.writes[TopN]
  implicit val nativeQueryWrites = Json.writes[NativeQueryRequest]
  implicit val customRefsWrites = Json.writes[CustomRefs]

  object QueryReqToJson {
    def unapply(s: QueryReq): Option[JsValue] = Some(queryReqFormat.writes(s))
  }
  val likes = (0 to 170).map(i => s"TITLE like '%992$i%'").reduce(_ + " or " + _)

  val longRunningQuery = s"""select TITLE, ID from TMP where $likes"""

  // quoting names
  val longRunningNativeQuery = s"""select TITLE, ID from "foodmart"."TMP" where $likes"""

  val qq = "\"\"\""
  def provideBaseSchema(): (CatalogId, DataSourceId, SchemaId) = {
    val (cat, ds, s) = createFoodMartBasedSchemaReturningAll(
      "Foodmart",
      s"""schema Foodmart (dataSource = "foodmart") {
         |  measure tc(column = "sales_fact_slow.ID")
         |  measure unitSales(column = "sales_fact_1998.unit_sales")
         |  table sales_fact_slow(sql =$qq$longRunningNativeQuery$qq") {
         |    column ID
         |  }
         |  table sales_fact_1998 {
         |    column unit_sales
         |  }
         |  table TMP {
         |    column TITLE
         |    column ID
         |  }
         |}
    """.stripMargin
    )
    (cat.catalogId, ds.dataSourceId, s.schemaId)
  }

  val fastAggregateQuery = Json.obj("type" -> "Aggregate", "measures" -> Seq("unitSales"), "limit" -> 1)

  def runAggregateQuery(catId: UUID, schemaId: UUID, query: JsObject, customRef: Option[String] = None): Future[Result] =
    route(
      app,
      FakeRequest(POST, s"/catalogs/$catId/schemas/$schemaId/query")
        .withJsonBody(query ++ customRef.fold(Json.obj())(ref => Json.obj("customReference" -> ref)))
    ).get

  "Query history endpoint" should {

    "list pagination" in {
      val (catId, _, schemaId) = provideBaseSchema()

      testPagination[QueryHistoryRecord](
        app,
        Some(catId),
        "queryHistory",
        _ => Await.ready(runAggregateQuery(catId, schemaId, fastAggregateQuery), 10.seconds),
        () => truncate("query_history")
      )
    }
    /*
     * Testing GET and GET/id  endpoint is one test
     * because we have long running queries here and slick rejects execution when making these tests separate and using BaseOneAppPerTest
     * This is known problem : https://github.com/slick/slick/issues/1183
     */
    "provide information on running queries and functionality for query cancelling" in {
      implicit val pc = new PatienceConfig(scaled(Span(40, Seconds)), scaled(Span(200, Millis)))

      val (catId, dsId, schemaId) = provideBaseSchema()

      prepareTmpTable()

      //////Calling APIs, queries get registered and then start executing///////////////////

      val fastAggregateQueryResult = runAggregateQuery(catId, schemaId, fastAggregateQuery, Some("fastAggregate"))


      val slowAggregateQuery = Json.obj("type" -> "Aggregate", "measures" -> Seq("tc"))
      runAggregateQuery(catId, schemaId, slowAggregateQuery, Some("slowAggregate"))


      val sqlQuery =Json.obj("type" -> "Sql", "sql" -> longRunningQuery)
      route(
        app,
        FakeRequest(POST, s"/catalogs/$catId/schemas/$schemaId/query")
          .withJsonBody(sqlQuery ++ Json.obj("customReference" -> "sql"))
      ).get

      val failingSqlQuery =Json.obj("type" -> "Sql", "sql" -> "select x from foo")
      route(
        app,
        FakeRequest(POST, s"/catalogs/$catId/schemas/$schemaId/query")
          .withJsonBody( failingSqlQuery ++ Json.obj("customReference" -> "failingSql"))
      ).get

      val savedQuery = SavedQueriesRow(schemaId, slowAggregateQuery.toString(), UUID.randomUUID(), catId)

      {
        val sqId = createSavedQuery(savedQuery).savedQueryId
        route(app,
              FakeRequest(POST, s"/catalogs/$catId/savedQueries/$sqId/execute")
                .withJsonBody(Json.obj("_customReference" -> "saved"))).get
      }

      val nativeUUID = UUID.randomUUID()
      val nativeQuery = longRunningNativeQuery
      route(
        app,
        FakeRequest(POST, s"/catalogs/$catId/dataSources/${dsId}/nativeQuery")
          .withBody(new ~(NativeQueryRequest(nativeQuery), CustomRefs(Some(nativeUUID), Some("native"))))
      ).get

      ////////////////////////////////////////////////////////////////////////////////////////////

      val numberOfQueries = 6

      // waiting for all queries to be registered
      val initialRunningQueries = eventually {
        val rc = getRegisteredQueries(app, catId)
        rc.size mustBe numberOfQueries
        rc
      }


      initialRunningQueries.map(_.id).distinct.size mustBe numberOfQueries

      initialRunningQueries.collect {
        case NativeHistoryRecord(
            //testing UUID manual setting
            `nativeUUID`,
            _,
            `longRunningNativeQuery`,
            `dsId`,
            None,
            None,
            None,
            None,
            None,
            `catId`,
            Some("native"),
            QueryHistoryRecordType.Native
            ) =>
          ()
      }.size mustBe 1


      initialRunningQueries.collect {
        case SchemaBasedHistoryRecord(
            _,
            _,
            QueryReqToJson(`slowAggregateQuery`),
            `schemaId`,
            None,
            None,
            None,
            None,
            None,
            `catId`,
            Some("slowAggregate"),
            QueryHistoryRecordType.Schema
            ) =>
          ()
      }.size mustBe 1

      initialRunningQueries.collect {
        case SchemaBasedHistoryRecord(
            _,
            _,
            QueryReqToJson(`sqlQuery`),
            `schemaId`,
            None,
            None,
            None,
            None,
            None,
            `catId`,
            Some("sql"),
            QueryHistoryRecordType.Schema
            ) =>
          ()
      }.size mustBe 1

      val savedQueryBase = Json.parse(savedQuery.query)
      initialRunningQueries.collect {
        case SchemaBasedHistoryRecord(
            _,
            _,
            QueryReqToJson(`savedQueryBase`),
            `schemaId`,
            None,
            None,
            None,
            None,
            None,
            `catId`,
            Some("saved"),
            QueryHistoryRecordType.Schema
            ) =>
          ()
      }.size mustBe 1


      initialRunningQueries.collect {
        case SchemaBasedHistoryRecord(
            _,
            _,
            QueryReqToJson(`fastAggregateQuery`),
            `schemaId`,
            //Ignoring completion status, completion time, and plans because fast Aggregate query may be already completed at this point
            _,
            _,
            _,
            _,
            _,
            `catId`,
            Some("fastAggregate"),
            QueryHistoryRecordType.Schema
            ) =>
          ()
      }.size mustBe 1

      eventually(
        getRegisteredQueries(app, catId).collect {
          case SchemaBasedHistoryRecord(
          _,
          _,
          QueryReqToJson(`failingSqlQuery`),
          `schemaId`,
          Some(Failed("java.lang.RuntimeException: Table 'foo' not found")),
          _,
          None,
          None,
          None,
          `catId`,
          Some("failingSql"),
          QueryHistoryRecordType.Schema
          ) =>
            ()
        }.size mustBe 1
      )




      val hasPlans = (Set("native", "failingSql").contains _).andThen(v => !v)
      // checking that history gets updated with plans
      val queriesWithPlans = eventually {
        val qwps =
          initialRunningQueries
            .filter(_.customReference.forall(hasPlans))
            .map(q =>
              contentAsJson(route(app, FakeRequest(GET, s"/catalogs/$catId/queryHistory/${q.id}")).get)
                .as[QueryHistoryRecord])

        qwps.foreach { qwp =>
          qwp.backendPhysicalPlan mustBe 'defined
          qwp.backendLogicalPlan mustBe 'defined
        }

        qwps.filter(v => v.customReference.contains("slowAggregate") || v.customReference.contains("fastAggregate")).foreach {
          qwp =>
            qwp.plan mustBe 'defined
        }
        qwps
      }

      //checking plans of one of the queries
      val resp = queriesWithPlans.collect {
        case r: SchemaBasedHistoryRecord if r.customReference.contains("slowAggregate") => r
      }.head

      resp.query.`type` mustBe Aggregate

      resp.plan mustBe
        Some("""Limit[offset=0]
               |  Sort[fields=[]]
               |    Project[cols=[c(tc)]
               |      Aggregate[group=[], aggregations=[Agg(Sum, tc)]]
               |        Project[cols=[c(tc)]
               |          View[table=sales_fact_slow, expressions=[]]""".stripMargin)

      resp.backendLogicalPlan.map(_.replaceFirst("condition=.*?\\]", "...")) mustBe
        Some("""LogicalProject(tc=[$0])
               |  LogicalAggregate(group=[{}], tc=[SUM($0)])
               |    LogicalProject(tc=[$0])
               |      LogicalFilter(...)
               |        JdbcTableScan(table=[[foodmart, TMP]])
               |""".stripMargin)

      resp.backendPhysicalPlan.map(_.replaceFirst("condition=.*?\\]", "...")) mustBe
        Some("""JdbcToEnumerableConverter
               |  JdbcAggregate(group=[{}], tc=[SUM($0)])
               |    JdbcProject(tc=[$0])
               |      JdbcFilter(...)
               |        JdbcTableScan(table=[[foodmart, TMP]])
               |""".stripMargin)

      // waiting for short query to complete in order to _hope_ (is it possible to make sure?) that queries are launched in db(need this step to properly test cancellation)
      eventually(fastAggregateQueryResult.isCompleted mustBe true)

      //  short query is completed while others are running
      getRegisteredQueries(app, catId).count(v => v.completedAt.isEmpty && v.completionStatus.isEmpty) mustBe 4

      val completedQueries =
        getRegisteredQueries(app, catId).filter(v => v.completedAt.isDefined && v.completionStatus.isDefined)

      completedQueries.size mustBe 2

      val completedSucceededQuery = completedQueries.filterNot(_.customReference.contains("failingSql")).head
      completedSucceededQuery.isInstanceOf[SchemaBasedHistoryRecord] mustBe true

      val c = completedSucceededQuery.asInstanceOf[SchemaBasedHistoryRecord]

      c.completionStatus.get mustBe Succeeded
      c.query.`type` mustBe Aggregate
      QueryReqToJson.unapply(c.query) mustBe Some(fastAggregateQuery)

      // testing customRef filters
      getRegisteredQueries(app, catId, Some("f%25te")).flatMap(_.customReference) mustBe Seq("fastAggregate")
      getRegisteredQueries(app, catId, Some("%25Aggregate"))
        .flatMap(_.customReference) must contain only ("slowAggregate", "fastAggregate")

      //TODO: cover 'cancel' endpoint when it is fully supported (or when we have a test case for Spark)
    }
  }
}
