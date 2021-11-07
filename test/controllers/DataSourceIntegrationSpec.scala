package controllers

import java.util.UUID

import dao.Tables._
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test._
import _root_.util.Fixtures
import _root_.util.JsonSerializers.uriFormat
import _root_.util.DBIOUtil._
import Writables.jsonWritable
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.play.BaseOneAppPerSuite
import DataSourceController._
import controllers.helpers.CrudlActions.PagedResponse

class DataSourceIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  val url = "jdbc:i.nl"

  import profile.api._
  import Fixtures.pagedResponseReads

  implicit val dataSourceRespReads = Json.reads[DataSourceResp]
  implicit val dataSourceReqWrites = Json.writes[DataSourceReq]

  def createDs(typ: String = "jdbc",
               _url: Option[String] = Some(url)): (CatalogsRow, DataSourcesRow, DataSourceProductsRow) = {
    val c = createCatalog()
    val (dsProd, _) = createDataSourceProduct(typ)
    val ds = createDataSource(c.catalogId,
                              "nme",
                              _url.map(u => Map("url" -> u)).getOrElse(Map.empty),
                              Some(dsProd.dataSourceProductId))
    (c, ds, dsProd)
  }

  def createDs1() = {
    val (c, ds, _) = createDs()
    (c.catalogId, ds.dataSourceId, ds.name)
  }

  def genId() = UUID.randomUUID()
  val unusedId = genId()

  "DataSourceController: positive cases" should {

    "get by key" in {
      val (c, ds, dsProd) = createDs()
      val (cid, dsId, dsName) = (c.catalogId, ds.dataSourceId, ds.name)

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/dataSources/$dsId")).get

      contentAsJson(show).as[DataSourceResp] mustBe DataSourceResp.fromRowWithProducts(ds, dsProd)
      status(show) mustBe OK
      contentType(show) mustBe Some("application/json")
    }

    "test connection" in {
      val catalog = createCatalog()
      val ds = createFoodmartDS(catalog.catalogId)

      val test = route(app,
                       FakeRequest(GET,
                                   s"/catalogs/${catalog.catalogId}/dataSources/${ds.dataSourceId}/testConnection")).get
      status(test) mustBe OK
      contentType(test) mustBe Some("application/json")
      contentAsJson(test).as[ConnectionTestResult] mustBe ConnectionTestResult(true)
    }

    "list" in {
      val (c, ds, dsProd) = createDs()
      val (cid, dsId, dsName) = (c.catalogId, ds.dataSourceId, ds.name)
      val ds2 = createDataSource(cid, "nme1", Map("url" -> url), Some(dsProd.dataSourceProductId))

      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/dataSources")).get

      contentAsJson(show)
        .as[PagedResponse[DataSourceResp]].data mustBe
          Seq((ds, dsProd), (ds2, dsProd)).map(DataSourceResp.fromRowWithProducts)
    }
    "list pagination" in {
      val cid = createCatalog().catalogId
      import scala.concurrent.duration._
      testPagination[DataSourceResp](
        app,
        Some(cid),
        "dataSources",
        i => createDataSource(cid, s"DS$i", Map("url" -> url)),
        () => truncate("data_sources")
      )
    }

    "delete by key" in {
      val (c, ds, _) = createDs()
      val (cid, dsId, dsName) = (c.catalogId, ds.dataSourceId, ds.name)

      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/dataSources/$dsId")).get
      // check response
      status(delete) mustBe OK
      // check record has been removed
      await(DataSources.filter(ds => ds.catalogId === cid && ds.name === dsName).result.headOption.run(db)) mustBe None
    }

    val dsTypeAndUriTable = Table(
      "dsType" -> "uri",
      "jdbc" -> Map("url" -> url),
      "druid" -> Map("url" -> url),
      "reflective" -> Map.empty
    )
    //need coordinationUrl for druid, target for reflective
    val params = Map("coordinatorUrl" -> "a.b", "target" -> "xx")

    "update by key" in {

      forAll(dsTypeAndUriTable) { (dsType, _uri) =>
        val (c, ds, _) = createDs()
        val (cid, dsId, dsName) = (c.catalogId, ds.dataSourceId, ds.name)

        val c1 = createCatalog()
        val (dsProd, _) = createDataSourceProduct(dsType)
        val upd =
          DataSourceReq(dsName, Some("3"), params ++ _uri, Some(unusedId), dsProd.dataSourceProductId, Some(unusedId))
        val update = route(app, FakeRequest(PUT, s"/catalogs/$cid/dataSources/$dsId").withBody(upd)).get
        contentAsJson(update).as[DataSourceReq] mustBe upd.copy(id = Some(dsId), catalogId = Some(cid))
        status(update) mustBe OK
        contentType(update) mustBe Some("application/json")
      }
    }

    "create" in {
      forAll(dsTypeAndUriTable) { (dsType, _uri) =>
        val c = createCatalog()
        val cid = c.catalogId
        val (dsProd, _) = createDataSourceProduct(dsType)

        //id provided
        val crt = DataSourceReq("new", None, params ++ _uri, Some(cid), dsProd.dataSourceProductId, Some(genId()))
        val create = route(app, FakeRequest(POST, s"/catalogs/$cid/dataSources").withBody(crt)).get
        status(create) mustBe CREATED
        contentType(create) mustBe Some("application/json")
        contentAsJson(create).as[DataSourceReq] mustBe crt

        //id is generated by server
        val crt2 = DataSourceReq("new2", None, params ++ _uri, Some(cid), dsProd.dataSourceProductId, None)
        val createNoId =
          route(
            app,
            FakeRequest(POST, s"/catalogs/$cid/dataSources").withBody(crt2)
          ).get
        status(createNoId) mustBe CREATED
        (contentAsJson(createNoId) \ "id").isDefined mustBe true
      }
    }
  }
  "DataSourceController: negative cases" should {
    "return NOT_FOUND if DataSource does not exist during show, update, delete" in {
      val c = createCatalog()
      val (dsProd, _) = createDataSourceProduct()
      val cid = c.catalogId
      val inexistentId = UUID.randomUUID()
      val show = route(app, FakeRequest(GET, s"/catalogs/$cid/dataSources/$inexistentId")).get
      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/dataSources/$inexistentId")).get
      val update = {
        val upd =
          DataSourceReq("INEXISTENT", Some("3"), Map(), Some(unusedId), dsProd.dataSourceProductId, Some(unusedId))
        route(app, FakeRequest(PUT, s"/catalogs/$cid/dataSources/$inexistentId").withBody(upd)).get
      }

      val strErr = s"""{"errors":["entity with id: 'DataSource '$inexistentId' from catalog '$cid'' not found"]}"""
      contentAsString(show) mustBe strErr
      status(show) mustBe NOT_FOUND

      contentAsString(delete) mustBe strErr
      status(delete) mustBe NOT_FOUND

      contentAsString(show) mustBe strErr
      status(update) mustBe NOT_FOUND

    }

    "return BAD_REQUEST when trying to detete Datasource that is used in schema" in {
      val (cid, dsId, dsName) = createDs1()
      createSchema(cid, emptyPsl("sName"), Seq(dsId))
      val delete = route(app, FakeRequest(DELETE, s"/catalogs/$cid/dataSources/$dsId")).get
      // check response
      contentAsString(delete) mustBe """{"errors":["cannot delete DataSource.Reason:DataSource is used in existing schemas: [sName]"]}"""
      status(delete) mustBe UNPROCESSABLE_ENTITY
      contentType(delete) mustBe Some("application/json")
    }

    "return BAD_REQUEST when trying to update jdbc, druid datasource with empty location" in {
      val (c, ds, _) = createDs("reflective", None)
      forAll(Table("DataSource type", "jdbc", "druid")) { dsType =>
        val (dsProd, _) = createDataSourceProduct(dsType)
        val response = {
          val crt = DataSourceReq(ds.name, None, Map(), Some(unusedId), dsProd.dataSourceProductId, Some(unusedId))
          route(app, FakeRequest(PUT, s"/catalogs/${c.catalogId}/dataSources/${ds.dataSourceId}").withBody(crt)).get
        }
        status(response) mustBe UNPROCESSABLE_ENTITY
        contentType(response) mustBe Some("application/json")
        contentAsString(response) mustBe s"""{"errors":["Cannot change [dataSourceProductId '${ds.dataSourceProductId}' to '${dsProd.dataSourceProductId}'] in DataSource '${ds.dataSourceId}' from catalog ${c.catalogId}. Reasons: [cannot build data source of type '$dsType' from props, 'url' must be defined]"]}"""
      }
    }

    "return BAD_REQUEST when trying to update datasource with unsupported type" in {
      val (c, ds, _) = createDs()
      val (cid, dsId, dsName) = (c.catalogId, ds.dataSourceId, ds.name)
      val (dsProd, _) = createDataSourceProduct("unknown")

      val response = {
        val crt = DataSourceReq(dsName, None, Map(), Some(unusedId), dsProd.dataSourceProductId, Some(unusedId))
        route(app, FakeRequest(PUT, s"/catalogs/$cid/dataSources/$dsId").withBody(crt)).get
      }
      status(response) mustBe UNPROCESSABLE_ENTITY
      contentType(response) mustBe Some("application/json")
      contentAsString(response) mustBe s"""{"errors":["Cannot change [properties 'Map(url -> $url)' to 'Map()',dataSourceProductId '${ds.dataSourceProductId}' to '${dsProd.dataSourceProductId}'] in DataSource '$dsId' from catalog ${cid}. Reasons: [cannot build data source of type 'unknown' from props, type is not supported]"]}"""
    }

    "return BAD_REQUEST when trying to create datasource with unsupported type" in {
      val c = createCatalog()
      val (dsProd, _) = createDataSourceProduct("unknown")

      val response = {
        val crt = DataSourceReq("r", None, Map(), Some(genId()), dsProd.dataSourceProductId, Some(genId()))
        route(app, FakeRequest(POST, s"/catalogs/${c.catalogId}/dataSources").withBody(crt)).get
      }
      status(response) mustBe UNPROCESSABLE_ENTITY
      contentType(response) mustBe Some("application/json")
      contentAsString(response) mustBe """{"errors":["Cannot create DataSource. Reason:[cannot build data source of type 'unknown' from props, type is not supported]"]}"""

    }

    "return BAD_REQUEST when trying to create jdbc, druid datasource with empty location" in {
      val c = createCatalog()
      forAll(Table("DataSource type", "jdbc", "druid")) { dsType =>
        val (dsProd, _) = createDataSourceProduct(dsType)

        val response = {
          val crt = DataSourceReq("r", None, Map(), Some(genId()), dsProd.dataSourceProductId, Some(genId()))
          route(app, FakeRequest(POST, s"/catalogs/${c.catalogId}/dataSources").withBody(crt)).get
        }
        status(response) mustBe UNPROCESSABLE_ENTITY
        contentType(response) mustBe Some("application/json")
        contentAsString(response) mustBe s"""{"errors":["Cannot create DataSource. Reason:[cannot build data source of type '$dsType' from props, 'url' must be defined]"]}"""
      }
    }

    "return BAD_REQUEST when trying to create DataSource with non-existent catalogId" in {
      val inxistentId = genId()
      val (dsProd, _) = createDataSourceProduct()

      val create = {
        val crt =
          DataSourceReq("new", None, Map("url" -> url), Some(genId()), dsProd.dataSourceProductId, Some(genId()))
        route(app, FakeRequest(POST, s"/catalogs/$inxistentId/dataSources").withBody(crt)).get
      }
      //TODO: return NOT_FOUND for such cases
      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")
      contentAsString(create) mustBe s"""{"errors":["Cannot create DataSource. Reason:[Catalog '$inxistentId' does not exist]"]}"""

    }
    "return BAD_REQUEST when trying to create DataSource with duplicate name within catalog" in {
      val (cid, _, dsName) = createDs1()
      val (dsProd, _) = createDataSourceProduct("druid")
      val create = {
        val crt =
          DataSourceReq(dsName,
                        None,
                        Map("url" -> url, "coordinatorUrl" -> "a.b"),
                        Some(genId()),
                        dsProd.dataSourceProductId,
                        Some(genId()))
        route(app, FakeRequest(POST, s"/catalogs/$cid/dataSources").withBody(crt)).get
      }
      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")
      contentAsString(create) mustBe s"""{"errors":["Cannot create DataSource. Reason:[DataSource 'nme' already exists in Catalog '$cid']"]}"""

    }

    "return BAD_REQUEST when trying to create DataSource from malformed json object" in {
      val (cid, _, dsName) = createDs1()
      val create = route(app,
                         FakeRequest(POST, s"/catalogs/$cid/dataSources")
                           .withBody(Json.obj("dataSourceId" -> 1, "a" -> "b"))).get

      status(create) mustBe UNPROCESSABLE_ENTITY
      contentType(create) mustBe Some("application/json")

      contentAsJson(create) mustBe Json.parse(
        """{"fieldErrors":{"properties":["is required"],"dataSourceProductId":["is required"],"name":["is required"]}}""")

    }
    "return BAD_REQUEST when trying to change name of Datasource that is used in schema" in {
      val (c, ds, _) = createDs()
      val (cid, dsId) = (c.catalogId, ds.dataSourceId)
      createSchema(cid, emptyPsl("sName"), Seq(dsId))
      val c1 = createCatalog()

      val updateCatalogId = {
        val upd =
          DataSourceReq("NewName", Some("3"), Map("url" -> url), Some(genId()), ds.dataSourceProductId, Some(genId()))
        route(app, FakeRequest(PUT, s"/catalogs/$cid/dataSources/$dsId").withBody(upd)).get
      }

      // check response
      status(updateCatalogId) mustBe UNPROCESSABLE_ENTITY
      contentType(updateCatalogId) mustBe Some("application/json")
      contentAsString(updateCatalogId) mustBe
        s"""{"errors":["Cannot change [name 'nme' to 'NewName'] in DataSource '$dsId' from catalog $cid. Reasons: [DataSource is used in existing schemas: [sName]]"]}"""
    }

    "return BAD_REQUEST when trying to change name of Datasource if datasource with the same name exists in catalog " in {
      val (c, ds, _) = createDs()
      val (cid, dsId) = (c.catalogId, ds.dataSourceId)

      val dsn = createDataSource(cid, "NewName", Map("url" -> url), Some(ds.dataSourceProductId))

      val updateName = {
        val upd =
          DataSourceReq("NewName", Some("3"), Map("url" -> url), Some(genId()), ds.dataSourceProductId, Some(genId()))
        route(app, FakeRequest(PUT, s"/catalogs/$cid/dataSources/$dsId").withBody(upd)).get
      }

      // check response
      status(updateName) mustBe UNPROCESSABLE_ENTITY
      contentType(updateName) mustBe Some("application/json")
      contentAsString(updateName) mustBe
        s"""{"errors":["Cannot change [name 'nme' to 'NewName'] in """ +
          s"""DataSource '$dsId' from catalog $cid. Reasons: [DataSource 'NewName' already exists in Catalog '$cid']"]}"""
    }
  }
}
