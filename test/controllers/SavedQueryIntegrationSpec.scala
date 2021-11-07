package controllers

import java.net.URI
import java.util.UUID

import _root_.util.DBIOUtil._
import _root_.util.Fixtures
import controllers.Writables.jsonWritable
import dao.Tables._
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.{AggregateQuery, SqlQuery}
import util.JsonSerializers.uriFormat
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test._
import services.{SavedQuery, SavedQueryRepo}
import SavedQuery.savedQueryFormat
import controllers.SavedQueryController.SavedQueryListResponse
import controllers.helpers.CrudlActions.PagedResponse
import services.serializers.QuerySerializers.QueryReq
import services.SchemaRepo.ParsedPsl

class SavedQueryIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  val uri = Some(URI.create("i.nl"))

  import profile.api._
  import Fixtures.pagedResponseReads

  private def createQuery() = {
    val catalogsRow = createCatalog()
    val s = createSchema(catalogsRow.catalogId, emptyPsl("X"), Seq())
    val pq = createSavedQuery(
      SavedQueriesRow(s.schemaId,
                      """{"type" :"Aggregate", "measures": ["x"], "filter": "x > :paramX"}""",
                      genId(),
                      catalogsRow.catalogId,
                      Some("name"),
                      Some("descr")))
    (catalogsRow, SavedQueryRepo.toSavedQuery(pq), pq.savedQueryId, s.schemaId)
  }

  def genId() = UUID.randomUUID()
  val unusedId = genId()

  val baseQuery = QueryReq.fromQuery(SqlQuery("foo"))

  "SavedQueryController: positive cases" should {

    val systemProps =
      """
        |     "_page" : {
        |        "type" : "integer",
        |        "minimum" : 1,
        |        "format" : "int32",
        |        "system" : true
        |      },
        |      "_pageSize" : {
        |        "type" : "integer",
        |        "minimum" : 1,
        |        "format" : "int32",
        |        "system" : true
        |      },
        |      "_queryId" : {
        |        "type" : "string",
        |        "format" : "uuid",
        |        "system" : true
        |      },
        |      "_customReference" : {
        |        "type" : "string",
        |        "maxLength" : 255,
        |        "system" : true
        |      }
      """.stripMargin

    "get by key" in {
      val (c, pq, pqId, sid) = createQuery()
      val cid = c.catalogId

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/savedQueries/$pqId")).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      contentAsJson(show) mustBe Json.parse(s"""{
                                              |  "name" : "name",
                                              |  "description" : "descr",
                                              |  "id" : "$pqId",
                                              |  "catalogId" : "$cid",
                                              |  "schemaId" : "$sid",
                                              |  "query" : {
                                              |    "type" : "Aggregate",
                                              |    "measures" : [ "x" ],
                                              |    "filter" : "x > :paramX"
                                              |  },
                                              |  "requestSchema" : {
                                              |    "type" : "object",
                                              |    "properties" : {
                                              |     "paramX" : {
                                              |        "type" : "string"
                                              |      },
                                              |      $systemProps
                                              |    },
                                              |    "required" : [ "paramX" ]
                                              |  }
                                              |}""".stripMargin)
    }

    "list" in {
      val (c, pq, pqId, sid) = createQuery()
      val (cid, pqDispName) = (c.catalogId, pq.name)
      val pq2 = createSavedQuery(
        SavedQueriesRow(sid, """{"type" : "Aggregate","measures": ["x"], "filter": "x > :userParam", "aggregateFilter": "x > :userAggParam and  x >= :userParam"}""", genId(), cid, Some("name1"), Some("descr1")))

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/savedQueries")).get
      contentAsJson(show) mustBe Json.parse(
        s"""
          |{
          |  "page" : {
          |    "current" : 1,
          |    "itemsPerPage" : 2147483647,
          |    "itemCount" : 2
          |  },
          |  "data" : [ {
          |    "name" : "name",
          |    "description" : "descr",
          |    "id" : "$pqId",
          |    "catalogId" : "${c.catalogId}",
          |    "schemaId" : "$sid",
          |    "query" : {
          |      "type" : "Aggregate",
          |      "measures" : [ "x" ],
          |      "filter" : "x > :paramX"
          |    },
          |    "requestSchema" : {
          |      "type" : "object",
          |      "properties" : {
          |        "paramX" : {
          |          "type" : "string"
          |        },
          |        $systemProps
          |      },
          |      "required" : [ "paramX" ]
          |    }
          |  }, {
          |    "name" : "name1",
          |    "description" : "descr1",
          |    "id" : "${pq2.savedQueryId}",
          |    "catalogId" : "${c.catalogId}",
          |    "schemaId" : "$sid",
          |    "query" : {
          |      "type" : "Aggregate",
          |      "measures" : [ "x" ],
          |      "filter" : "x > :userParam",
          |      "aggregateFilter": "x > :userAggParam and x >= :userParam"
          |    },
          |    "requestSchema" : {
          |      "type" : "object",
          |      "properties" : {
          |        "userParam" : {
          |          "type" : "string"
          |        },
          |        "userAggParam" : {
          |          "type" : "string"
          |        },
          |       $systemProps
          |      },
          |      "required" : [ "userParam", "userAggParam"]
          |    }
          |  } ]
          |}
        """.stripMargin
      )
    }

    "list pagination" in {
      val cid = createCatalog().catalogId

      val sid = {
        val schema =
          """schema X {
            |measure x(column = "t.x")
            |table t { column x}
            |}""".stripMargin
        createSchema(cid, ParsedPsl(schema).right.get, Seq()).schemaId
      }

      testPagination[SavedQuery](
        app,
        Some(cid),
        "savedQueries",
        i =>
          createSavedQuery(
            SavedQueriesRow(sid,
                            """{"type" : "Aggregate","measures": ["x"]}""",
                            genId(),
                            cid,
                            Some(s"name1$i"),
                            Some("descr1"))),
        () => truncate("saved_queries")
      )
    }

    "delete by key" in {
      val (c, pq, pqId, _) = createQuery()
      val (cid, name) = (c.catalogId, pq.name)

      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/savedQueries/$pqId")).get
      // check response
      status(delete) mustBe OK
      // check record has been removed
      await(SavedQueries.filter(q => q.catalogId === cid && q.name === name).result.headOption.run(db)) mustBe None
    }

    "update by key" in {

      val (c, pq, pqId, sid) = createQuery()
      val (cid, pqDispName) = (c.catalogId, pq.name)

      val upd = SavedQuery(unusedId, unusedId, sid, baseQuery, pqDispName, None)
      val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/savedQueries/$pqId").withBody(upd)).get
      status(update) mustBe OK
      contentType(update) mustBe Some("application/json")

      contentAsJson(update) mustBe Json.parse(
        s"""
          |{
          |  "name" : "name",
          |  "description" : null,
          |  "id" : "$pqId",
          |  "catalogId" : "$cid",
          |  "schemaId" : "$sid",
          |  "query" : {
          |    "type" : "Sql",
          |    "sql" : "foo"
          |  },
          |  "requestSchema" : {
          |    "type" : "object",
          |    "properties" : {
          |      $systemProps
          |    },
          |    "required" : [ ]
          |  }
          |}
        """.stripMargin)
    }

    "create" in {
      val c = createCatalog()
      val cid = c.catalogId
      val sid = createSchema(cid, emptyPsl("X"), Seq()).schemaId

      val crt = SavedQuery(id = genId(), catalogId = unusedId, schemaId = sid, query = baseQuery, name = Some("1"), description = Some("descr"))
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/savedQueries").withBody(crt)).get
      status(create) mustBe CREATED
      contentType(create) mustBe Some("application/json")

      contentAsJson(create) mustBe Json.parse(
        s"""
          |{
          |  "name" : "1",
          |  "description" : "descr",
          |  "id" : "${crt.id}",
          |  "catalogId" : "$cid",
          |  "schemaId" : "$sid",
          |  "query" : {
          |    "type" : "Sql",
          |    "sql" : "foo"
          |  },
          |  "requestSchema" : {
          |    "type" : "object",
          |    "properties" : {
          |      $systemProps
          |    },
          |    "required" : [ ]
          |  }
          |}
        """.stripMargin)

    }
  }
  "SavedQueryController: negative cases" should {
    "return NOT_FOUND if SavedQuery does not exist during show, update, delete" in {
      val c = createCatalog()
      val cid = c.catalogId
      val inexistentId = genId()
      val strErr = s"""{"errors":["entity with id: 'SavedQuery '$inexistentId' from catalog '$cid'' not found"]}"""

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/savedQueries/$inexistentId")).get
      status(show) mustBe NOT_FOUND
      contentAsString(show) mustBe strErr

      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/savedQueries/$inexistentId")).get
      status(delete) mustBe NOT_FOUND
      contentAsString(delete) mustBe strErr

      val update = {
        val upd = SavedQuery(unusedId, unusedId, genId(), baseQuery)
        route(app, FakeRequest(PUT, s"/catalogs/$cid/savedQueries/$inexistentId").withBody(upd)).get
      }
      status(update) mustBe NOT_FOUND
      contentAsString(show) mustBe strErr
    }

    "return BAD_REQUEST when trying to create SavedQuery with non-existent catalogId or schema " in {
      val nonExistentId = genId()
      val sid = createSchema(createCatalog().catalogId, emptyPsl("X"), Seq()).schemaId
      val create = {
        val crt = SavedQuery(unusedId, unusedId, sid, baseQuery)
        route(app, FakeRequest(POST, s"/catalogs/$nonExistentId/savedQueries").withBody(crt)).get
      }
      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")
      contentAsJson(create) mustBe
        Json.obj("errors" -> Seq(s"""schema $sid not found in catalog $nonExistentId"""))
    }

    "return BAD_REQUEST when trying to create/update SavedQuery from malformed json object" in {
      val (c, _, sqId, _) = createQuery()
      val create =
        route(
          app,
          FakeRequest(POST, s"/catalogs/${c.catalogId}/savedQueries").withBody(Json.obj("name" -> 1, "query" -> 1))).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      contentAsJson(create) mustBe
        Json.parse(
          """{"fieldErrors":{"name":["expected: string"],"query":["expected: object"],"schemaId":["is required"]}}""")

      val update =
        route(
          app,
          FakeRequest(PUT, s"/catalogs/${c.catalogId}/savedQueries/$sqId").withBody(Json.obj("name" -> 1, "query" -> 1))).get

      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")

      contentAsJson(update) mustBe
        Json.parse(
          """{"fieldErrors":{"name":["expected: string"],"query":["expected: object"],"schemaId":["is required"]}}""")
    }

    "return UNPROCESSABLE_ENTITY when trying to create/update SavedQuery from invalid query request" in {
      val c = createCatalog()
      val cid = c.catalogId
      val sid = createSchema(cid, emptyPsl("X"), Seq()).schemaId

      val crt = SavedQuery(genId(), unusedId, sid, QueryReq.fromQuery(AggregateQuery(List("foo"))), Some("1"), Some("descr"))
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/savedQueries").withBody(crt)).get
      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      val error = Json.parse("""{"errors":["base query is invalid: measures must not be empty in Pivoted query"]}""")
      contentAsJson(create) mustBe error

      val (c2, _, sqId, _) = createQuery()
      val update = route(app, FakeRequest(PUT, s"/catalogs/${c2.catalogId}/savedQueries/$sqId").withBody(crt)).get
      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")

      contentAsJson(update) mustBe error
    }

    "return UNPROCESSABLE_ENTITY when trying to create/update SavedQuery from incompatible query request" in {
      val (cid, sid) = createFoodMartBasedSchema(
        "X",
        """schema X (dataSource="foodmart"){
          | measure mes(column = "Foo.mes")
          | dimension d(table="Bar") {
          |  attribute attr
          | }
          | table Foo{
          |  column mes
          | }
          | table Bar{
          |  column attr
          | }
          |}
        """.stripMargin
      )

      val crt = SavedQuery(
        genId(),
        unusedId,
        sid,
        QueryReq.fromQuery(AggregateQuery(measures = List("mes"), rows = List("attr"))),
        Some("1"),
        Some("descr")
      )
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/savedQueries").withBody(crt)).get
      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      val error = Json.parse(s"""{"errors":["base query is not compatible with schema $sid"]}""")
      contentAsJson(create) mustBe error

      //creating query againt the same schema to update it later
      val createOk = route(app, FakeRequest(POST, s"/catalogs/$cid/savedQueries").withBody(crt.copy(query = QueryReq.fromQuery(AggregateQuery(measures = List("mes")))))).get
      status(createOk) mustBe CREATED
      val sqId = (contentAsJson(createOk) \ "id").as[UUID]

      val update = route(app, FakeRequest(PUT, s"/catalogs/${cid}/savedQueries/$sqId").withBody(crt)).get
      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")

      contentAsJson(update) mustBe error
    }

  }
}
