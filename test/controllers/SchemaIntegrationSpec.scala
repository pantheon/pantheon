package controllers

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

import _root_.util.Fixtures
import dao.Tables._
import play.api.libs.json._
import play.api.test.Helpers.{defaultAwaitTimeout => _, _}
import play.api.test._
import _root_.util.DBIOUtil._
import Writables.jsonWritable
import org.scalatestplus.play.BaseOneAppPerSuite
import services.SchemaRepo.ParsedPsl
import akka.util.Timeout
import controllers.SchemaController.{SchemaListResponse, SchemaReq}
import controllers.helpers.CrudlActions.PagedResponse

class SchemaIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  import profile.api._
  import Fixtures.pagedResponseReads

  def genId() = UUID.randomUUID()
  val unusedId = genId()

  implicit val timeout = Timeout(100, TimeUnit.SECONDS)
  def getCmpFields(sr: SchemaReq): (String, Option[String], String) =
    (sr.name.get, sr.description, sr.psl)

  def schema(dsName: String, schemaName: String, imports: Seq[String] = Nil): ParsedPsl =
    parsePsl(s"""schema $schemaName (dataSource = "$dsName") {
       | ${if (imports.isEmpty) "" else imports.map(n => s"import {$n}").mkString("\n")}
       |  dimension C(table = "c") {
       |    attribute f(column = "f")
       |    attribute l(column = "l")
       |  }
       |  table c {
       |    column f
       |    column l
       |  }
       |}
       |""".stripMargin.replaceAll("\\n", "").replaceAll("\\s+", " "))

  def createDataSource(cid: UUID) =
    super.createDataSource(cid, "nme", Map("url" -> "jdbc:i.nl"))

  def createSchema() = {
    val c = createCatalog()
    val ds = createDataSource(c.catalogId)
    val s = super.createSchema(c.catalogId, schema(ds.name, "S"), Seq(ds.dataSourceId))
    (c, ds, s)
  }

  def getSchemaId(cid: UUID, name: String) =
    await(Schemas.filter(s => s.catalogId === cid && s.name === name).map(_.schemaId).result.head.run(db))

  def createParent(cid: UUID, dsName: String, schemaName: String, imports: Seq[String]) = {
    // Creating parent
    val cr = SchemaReq(psl = schema(dsName, "PARENT", imports).raw)
    val parentRes = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

    status(parentRes) mustBe CREATED
    contentAsJson(parentRes).as[SchemaReq]
  }
  def createSchemaWithParent(): (CatalogsRow, DataSourcesRow, SchemasRow, SchemasRow) = {
    val (c, ds, s) = createSchema()
    val p = createParent(c.catalogId, ds.name, "ParentOf_" + s.name, Seq(s.name))
    // Converting ResponseEntity to Row because parent is created using SchemaController itself and returns ResponseEntity.
    // This is done to ensure that all links(like schemaUsages) are established during parent creation
    val pRow =
      SchemasRow(p.name.get, p.description, p.psl, c.catalogId, None, getSchemaId(c.catalogId, p.name.get))
    (c, ds, s, pRow)
  }

  "SchemaController: positive cases" should {

    "get by key" in {
      val (c, ds, s) = createSchema()
      val (cid, schemaId, schemaName) = (c.catalogId, s.schemaId, s.name)
      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/schemas/$schemaId")).get

      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
      getCmpFields(contentAsJson(show).as[SchemaReq]) mustBe (s.name, s.description, s.psl)
    }

    "list" in {
      val (c, ds, s) = createSchema()
      val (cid, schemaId, schemaName, psl) = (c.catalogId, s.schemaId, s.name, s.psl)
      val s2 = super.createSchema(cid, emptyPsl("sNme2"), Seq())

      val list = route(app, FakeRequest(GET, s"/catalogs/$cid/schemas")).get

      val pr = contentAsJson(list).as[PagedResponse[SchemaReq]]
      pr.page.current mustBe 1
      pr.page.itemsPerPage mustBe Int.MaxValue
      pr.page.itemCount mustBe 2
      pr.data.map(getCmpFields) mustBe Seq(
        (s.name, s.description, "schema ..."),
        (s2.name, s2.description, "schema ...")
      )
    }

    "list paginated" in {
      val cid = createCatalog().catalogId
      testPagination[SchemaReq](
        app,
        Some(cid),
        "schemas",
        i => super.createSchema(cid, emptyPsl("S" + i), Seq()),
        () => truncate("schemas")
      )
    }

    "delete by key" in {
      val (c, ds, child, parent) = createSchemaWithParent()
      await(SchemaUsages.result.run(db)) mustBe Seq(SchemaUsagesRow(parent.schemaId, child.schemaId))
      val (cid, schemaId, schemaName) = (c.catalogId, parent.schemaId, parent.name)
      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/schemas/$schemaId")).get
      // check response
      status(delete) mustBe OK
      // check that schema, SchemaUsages and DataSourceLinks are removed
      await(Schemas.filter(s => s.catalogId === cid && s.name === schemaName).result.headOption.run(db)) mustBe None
      await(DataSourceLinks.filter(_.schemaId === parent.schemaId).result.run(db)) mustBe Seq()
      await(SchemaUsages.result.run(db)) mustBe Seq()
    }

    "update by key" in {

      val c = createCatalog()
      val cid = c.catalogId
      val ds = createDataSource(c.catalogId)
      val ch1 = super.createSchema(c.catalogId, emptyPsl("CH1"), Nil)
      val ch2 = super.createSchema(c.catalogId, emptyPsl("CH2"), Nil)
      val parentName = "PARENT"
      val p = createParent(c.catalogId, ds.name, parentName, Seq("CH1", "CH2"))
      val pSchemaId = getSchemaId(c.catalogId, p.name.get)

      val ch3 = super.createSchema(c.catalogId, emptyPsl("CH3"), Nil)

      await(DataSourceLinks.result.run(db)) mustBe Seq(DataSourceLinksRow(pSchemaId, ds.dataSourceId))
      await(SchemaUsages.result.run(db)) mustBe Seq(SchemaUsagesRow(pSchemaId, ch1.schemaId),
                                                    SchemaUsagesRow(pSchemaId, ch2.schemaId))
      val ds1 = createDataSource(cid, "X", Map("url" -> "jdbc:i.il"))

      //changing children here
      val newPsl = schema(ds1.name, "PARENT", Seq("CH1", "CH3"))

      val upd = SchemaReq(description = Some("3"), psl = newPsl.raw)
      val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/schemas/${p.id.get}").withBody(upd)).get

      status(update) mustBe OK
      contentType(update) mustBe Some("application/json")
      getCmpFields(contentAsJson(update).as[SchemaReq]) mustBe (newPsl.parsed.name,
      upd.description,
      upd.psl)

      // check that DataSourceLinks and SchemaUsages are updated
      await(DataSourceLinks.result.run(db)) mustBe Seq(DataSourceLinksRow(pSchemaId, ds1.dataSourceId))
      await(SchemaUsages.result.run(db)) mustBe Seq(SchemaUsagesRow(pSchemaId, ch1.schemaId),
                                                    SchemaUsagesRow(pSchemaId, ch3.schemaId))
    }

    "create with user provided id" in {
      val (c, ds, child) = createSchema()
      val cid = c.catalogId

      val newSchema = schema(ds.name, "new", Seq(child.name))
      val cr = SchemaReq(id = Some(genId()), psl = newSchema.raw)
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

      status(create) mustBe CREATED
      contentType(create) mustBe Some("application/json")

      getCmpFields(contentAsJson(create).as[SchemaReq]) mustBe (newSchema.parsed.name, cr.description, cr.psl)

      //checking that schema , DataSourceLinks and SchemaUsages are created
      val schemaId = getSchemaId(cid, newSchema.parsed.name)
      await(Schemas.filter(s => s.catalogId === cid && s.name === newSchema.parsed.name).result.headOption.run(db)).isDefined mustBe (true)
      await(DataSourceLinks.filter(_.schemaId === schemaId).result.run(db)) mustBe Seq(
        DataSourceLinksRow(schemaId, ds.dataSourceId))
      await(SchemaUsages.result.run(db)) mustBe Seq(SchemaUsagesRow(schemaId, child.schemaId))
    }
  }

  "create with autogenerated id" in {
    val (c, ds, child) = createSchema()
    val cid = c.catalogId

    val newSchema = schema(ds.name, "new", Seq(child.name))
    val cr = SchemaReq(psl = newSchema.raw)
    val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

    status(create) mustBe CREATED
    contentType(create) mustBe Some("application/json")

    getCmpFields(contentAsJson(create).as[SchemaReq]) mustBe (newSchema.parsed.name, cr.description, cr.psl)
  }

  "create schema with 2 datasources" in {
    val c = createCatalog()
    val ds1 = super.createDataSource(c.catalogId, "ds1", Map("url" -> "jdbc:i.nl"))
    val ds2 = super.createDataSource(c.catalogId, "ds2", Map("url" -> "jdbc:i.nl"))
    val cid = c.catalogId

    val psl = parsePsl(s"""schema Foodmart(strict = false) {
                |  dimension C(table = "c1") {
                |    attribute f(column = "f")
                |    attribute l(column = "l")
                |  }
                |  table c1(dataSource = "ds1") {
                |    column f
                |    column l
                |  }
                |  table c2(dataSource = "ds1") {
                |    column f
                |    column l
                |  }
                |}
                |""".stripMargin)

    val cr = SchemaReq(psl = psl.raw)
    val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

    status(create) mustBe CREATED
    contentType(create) mustBe Some("application/json")
    getCmpFields(contentAsJson(create).as[SchemaReq]) mustBe (psl.parsed.name, cr.description, cr.psl)
  }

  "SchemaController: negative cases" should {
    "return NOT_FOUND if Schema does not exist during show, update, delete" in {
      val c = createCatalog()
      val cid = c.catalogId

      val inexistentId = genId()

      val strErr = s"""{"errors":["entity with id: 'Schema '$inexistentId' from catalog '$cid'' not found"]}"""

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/schemas/${inexistentId}")).get
      status(show) mustBe NOT_FOUND
      println(contentAsString(show))
      contentAsString(show) mustBe strErr

      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/schemas/${inexistentId}")).get
      status(delete) mustBe NOT_FOUND
      contentAsString(delete) mustBe strErr

      val update = {
        val upd = SchemaReq(description = Some("3"), psl = emptyPsl("INEXISTENT").raw)
        route(app, FakeRequest(PUT, s"/catalogs/$cid/schemas/$inexistentId").withBody(upd)).get
      }
      status(update) mustBe NOT_FOUND
      contentAsString(update) mustBe strErr

    }

    "return BAD_REQUEST when trying to detete schema which is a child of other schema" in {
      val (c, ds, s, _) = createSchemaWithParent()

      val delete = route(app, FakeRequest(DELETE, s"/catalogs/${c.catalogId}/schemas/${s.schemaId}")).get

      contentAsString(delete) mustBe
        s"""{"errors":["Cannot delete Schema '${s.schemaId}' from catalog '${c.catalogId}'.Reason: Schema has the child relation to the following parent schemas: [PARENT]"]}"""
      status(delete) mustBe UNPROCESSABLE_ENTITY
      contentType(delete) mustBe Some("application/json")
    }

    "return BAD_REQUEST when trying to change PSL of the schema if new PSL is containing non-existent datasources or schema names" in {
      val c = createCatalog()
      val cid = c.catalogId
      val ds = createDataSource(cid)
      val s = super.createSchema(cid, schema(ds.name, "S"), Seq(ds.dataSourceId))

      val upd = SchemaReq(
        description = Some("3"),
        psl = schema("INEXISTENTDS", s.name, Seq("INEXISTENTSCHEMA")).raw
      )
      val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/schemas/${s.schemaId}").withBody(upd)).get

      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")
      contentAsString(update) mustBe s"""{"errors":["Cannot change [PSL] in Schema '${s.schemaId}' from catalog '$cid'. Reasons: """ +
        s"""[Referenced children ([INEXISTENTSCHEMA]) not found in catalog '$cid',Referenced data sources ([INEXISTENTDS]) not found in catalog '$cid']."]}"""

    }

    "return BAD_REQUEST when trying to create schema with non-existent catalogId" in {
      val cid = UUID.randomUUID()
      val cr = SchemaReq(psl = emptyPsl("new").raw)
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")
      contentAsString(create) mustBe s"""{"errors":["Cannot create schema. Reason: [Catalog '$cid' does not exist]"]}"""
    }

    "return BAD_REQUEST when trying to create schema having existing name within catalogId" in {
      val (c, _, s) = createSchema()
      val cid = c.catalogId
      val cr = SchemaReq(psl = emptyPsl(s.name).raw)
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      contentAsString(create) mustBe s"""{"errors":["Cannot create schema. Reason: [Schema '${s.name}' already exists within catalog '$cid']"]}"""
    }

    "return BAD_REQUEST when trying to create schema having PSL containing non-existent datasources or schema names" in {
      val c = createCatalog()
      val cid = c.catalogId
      val cr = SchemaReq(psl = schema("INEXISTENTDS", "new", Seq("INEXISTENTSCHEMA")).raw)
      val create = route(app, FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      contentAsString(create) mustBe s"""{"errors":["Cannot create schema. Reason: [Referenced children """ +
        s"""([INEXISTENTSCHEMA]) not found in catalog '$cid',Referenced data sources ([INEXISTENTDS]) not found in catalog '$cid']"]}"""
    }

    "return BAD_REQUEST when trying to create schema having malformed PSL" in {
      val c = createCatalog()
      val cid = c.catalogId
      val cr = SchemaReq(psl = "asdasdd")
      val create = route(
        app,
        FakeRequest(POST, s"/catalogs/$cid/schemas").withBody(cr)
      ).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")
      contentAsString(create) mustBe ("""{"errors":["missing 'schema' at 'asdasdd' at line:1, charPosition:0"]}""")
    }

    "return BAD_REQUEST when trying to change name of the schema if schema with the same name exists in catalog" in {
      val (c, ds, s) = createSchema()
      val (cid, schemaId, schemaName, dsId, dsName) = (c.catalogId, s.schemaId, s.name, ds.dataSourceId, ds.name)

      super.createSchema(c.catalogId, schema(dsName, "new"), Seq(dsId))

      val upd = SchemaReq(description = Some("3"), psl = emptyPsl("new").raw)
      val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/schemas/$schemaId").withBody(upd)).get

      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")
      contentAsString(update) mustBe s"""{"errors":["Cannot change [name 'S' to 'new',PSL] in Schema '$schemaId' """ +
        s"""from catalog '$cid'. Reasons: [Schema 'new' already exists within catalog '$cid']."]}"""

    }

    "return BAD_REQUEST when trying to change name of the schema which is a child of other schema" in {
      val (c, ds, s, _) = createSchemaWithParent()
      val (cid, schemaId, schemaName, dsId, dsName) = (c.catalogId, s.schemaId, s.name, ds.dataSourceId, ds.name)

      val upd = SchemaReq(description = Some("3"), psl = emptyPsl("new").raw)
      val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/schemas/$schemaId").withBody(upd)).get

      status(update) mustBe UNPROCESSABLE_ENTITY
      contentType(update) mustBe Some("application/json")
      contentAsString(update) mustBe s"""{"errors":["Cannot change [name 'S' to 'new',PSL] in Schema '$schemaId' """ +
        s"""from catalog '$cid'. Reasons: [Schema has the child relation to the following parent schemas: [PARENT]]."]}"""
    }
  }
}
