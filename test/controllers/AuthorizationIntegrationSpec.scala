package controllers

import java.net.URI
import java.util.UUID

import dao.Tables._
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test._
import org.scalatest.EitherValues._
import org.scalatestplus.play.BaseOneAppPerSuite
import _root_.util.Fixtures
import _root_.util.Fixtures.pagedResponseReads
import services.Authorization.PermissionTO
import Writables.jsonWritable
import AuthorizationControllerCommons.{permReads, permWrites}
import config.Authentication.User
import controllers.BackendConfigController.{BackendConfigListResponse, BackendConfigReq}
import play.api.inject.DefaultApplicationLifecycle
import play.api.{ApplicationLoader, Configuration}
import play.core.DefaultWebCommands
import controllers.CatalogController.{CatalogReq, reqRespFormat => catalogFormat}
import controllers.DataSourceController.{DataSourceReq, DataSourceResp}
import controllers.DataSourceProductController.DataSourceProductReq
import controllers.helpers.CrudlActions.PagedResponse

class AuthorizationIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  import profile.api._

  implicit val dataSourceReqWrites = Json.writes[DataSourceReq]
  override lazy val appContext = ApplicationLoader.Context(
    environment = env,
    sourceMapper = None,
    webCommands = new DefaultWebCommands(),
    initialConfiguration = Configuration.load(env) ++ Configuration("pantheon.auth.enabled" -> true),
    lifecycle = new DefaultApplicationLifecycle()
  )

  def createCatalog(user: User): Either[Int, CatalogReq] = {
    val res = route(app,
                    FakeRequest(POST, s"/catalogs")
                      .withUser(user)
                      .withJsonBody(Json.obj("name" -> randomStr(10), "description" -> randomStr(10)))).get
    val st = status(res)
    if (st == CREATED) Right(contentAsJson(res).as[CatalogReq])
    else Left(st)
  }

  def createDataSource(user: User, catalog: CatalogReq): Either[Int, DataSourceReq] = {
    val (prod, _) = createDataSourceProduct()
    val res = route(
      app,
      FakeRequest(POST, s"/catalogs/${catalog.id.get}/dataSources")
        .withUser(user)
        .withJsonBody(Json.toJson(
          DataSourceReq(randomStr(10), None, Map("url" -> "jdbc:i.nl"), catalog.id, prod.dataSourceProductId, None)))
    ).get
    val st = status(res)
    if (st == CREATED) Right(contentAsJson(res).as[DataSourceReq])
    else Left(st)
  }

  def createBackendConfig(user: User, catalog: CatalogReq): Either[Int, BackendConfigReq] = {
    val res = route(
      app,
      FakeRequest(POST, s"/catalogs/${catalog.id.get}/backendConfigs")
        .withUser(user)
        .withJsonBody(Json.toJson(BackendConfigReq(Some(randomStr(10)), "", params = Map())))
    ).get
    val st = status(res)
    if (st == CREATED) Right(contentAsJson(res).as[BackendConfigReq])
    else Left(st)
  }

  def createDataSourceProduct(user: User): Either[Int, DataSourceProductReq] = {
    val res = route(
      app,
      FakeRequest(POST, s"/dataSourceProducts")
        .withUser(user)
        .withJsonBody(
          Json.obj("name" -> randomStr(10), "properties" -> Json.arr(),
            "productRoot" -> randomStr(10), "isBundled" -> false, "type" -> "jdbc"))).get
    val st = status(res)
    if (st == CREATED) Right(contentAsJson(res).as[DataSourceProductReq])
    else Left(st)
  }

  def addPermission(user: User, resourceUrl: String, principalId: UUID, role: String): PermissionTO = {
    var perm = PermissionTO(UUID.randomUUID(), principalId, role)
    val create = route(app,
                       FakeRequest(POST, resourceUrl + "/permissions")
                         .withUser(user)
                         .withBody(perm)).get
    status(create) mustBe CREATED
    contentType(create) mustBe Some("application/json")
    contentAsJson(create).as[PermissionTO] mustBe perm
    perm
  }

  def checkDataSourceProdRudl(user: User,
                        dsProd: DataSourceProductReq,
                        existsInList: Boolean = true,
                        getStatus: Int = OK,
                        updateStatus: Int = OK,
                        deleteStatus: Int = OK): Unit = {

    // list
    {
      val res = route(app, FakeRequest(GET, s"/dataSourceProducts").withUser(user)).get
      status(res) mustBe OK
      contentAsJson(res).as[PagedResponse[DataSourceProductReq]].data.filterNot(_.isBundled) mustBe (
        if (existsInList) Seq(dsProd) else Seq.empty)
    }

    // find jdbcProduct
    {
      val res = route(app, FakeRequest(GET, s"/dataSourceProducts/${dsProd.id.get}").withUser(user)).get
      status(res) mustBe getStatus
      if (getStatus == OK) {
        contentAsJson(res).as[DataSourceProductReq] mustBe dsProd
      }
    }

    // update
    {
      val modifiedJdbcProd = dsProd.copy(name = randomStr(10))
      val res = route(app,
                      FakeRequest(PUT, s"/dataSourceProducts/${dsProd.id.get}")
                        .withUser(user)
                        .withJsonBody(Json.toJson(modifiedJdbcProd))).get
      status(res) mustBe updateStatus
      if (updateStatus == OK) {
        contentAsJson(res).as[DataSourceProductReq] mustBe modifiedJdbcProd
      }
    }

    // delete
    {
      val res = route(app, FakeRequest(DELETE, s"/dataSourceProducts/${dsProd.id.get}").withUser(user)).get
      status(res) mustBe deleteStatus
    }
  }

  def checkCatalogRudl(user: User,
                       catalog: CatalogReq,
                       existsInList: Boolean = true,
                       getStatus: Int = OK,
                       updateStatus: Int = OK,
                       deleteStatus: Int = OK): Unit = {

    // list catalogs
    {
      val res = route(app, FakeRequest(GET, s"/catalogs").withUser(user)).get
      status(res) mustBe OK
      contentAsJson(res).as[PagedResponse[CatalogReq]].data mustBe (
        if (existsInList) Seq(catalog) else Seq.empty)
    }

    // find catalog without realm is prohibited
    {
      val res = route(app, FakeRequest(GET, s"/catalogs/${catalog.id.get}").withUser(user)).get
      status(res) mustBe getStatus
      if (getStatus == OK) {
        contentAsJson(res).as[CatalogReq] mustBe catalog
      }
    }

    // update catalog
    {
      val modifiedCatalog = catalog.copy(name = Some(randomStr(10)))
      val res = route(app,
                      FakeRequest(PUT, s"/catalogs/${catalog.id.get}")
                        .withUser(user)
                        .withJsonBody(Json.toJson(modifiedCatalog))).get
      status(res) mustBe updateStatus
      if (updateStatus == OK) {
        contentAsJson(res).as[CatalogReq] mustBe modifiedCatalog
      }
    }

    // delete catalog
    {
      val res = route(app, FakeRequest(DELETE, s"/catalogs/${catalog.id.get}").withUser(user)).get
      status(res) mustBe deleteStatus
    }
  }

  def checkDataSourceRudl(user: User,
                          dataSource: DataSourceReq,
                          existsInList: Boolean = true,
                          getStatus: Int = OK,
                          updateStatus: Int = OK,
                          deleteStatus: Int = OK): Unit = {

    // list dataSources
    {
      val res = route(app, FakeRequest(GET, s"/catalogs/${dataSource.catalogId.get}/dataSources").withUser(user)).get
      status(res) mustBe OK
      contentAsJson(res).as[PagedResponse[DataSourceReq]].data mustBe (
        if (existsInList) Seq(dataSource) else Seq.empty)
    }

    // find dataSource without realm is prohibited
    {
      val res = route(
        app,
        FakeRequest(GET, s"/catalogs/${dataSource.catalogId.get}/dataSources/${dataSource.id.get}").withUser(user)).get
      status(res) mustBe getStatus
      if (getStatus == OK) {
        contentAsJson(res).as[DataSourceReq] mustBe dataSource
      }
    }

    // update dataSource
    {
      val modifiedDataSource = dataSource.copy(name = randomStr(10))
      val res = route(
        app,
        FakeRequest(PUT, s"/catalogs/${dataSource.catalogId.get}/dataSources/${dataSource.id.get}")
          .withUser(user)
          .withJsonBody(Json.toJson(modifiedDataSource))
      ).get
      status(res) mustBe updateStatus
      if (updateStatus == OK) {
        contentAsJson(res).as[DataSourceReq] mustBe modifiedDataSource
      }
    }

    // delete dataSource
    {
      val res = route(app,
                      FakeRequest(DELETE, s"/catalogs/${dataSource.catalogId.get}/dataSources/${dataSource.id.get}")
                        .withUser(user)).get
      status(res) mustBe deleteStatus
    }
  }

  def checkBackendConfigRudl(user: User,
                             backendConfig: BackendConfigReq,
                             existsInList: Boolean = true,
                             getStatus: Int = OK,
                             updateStatus: Int = OK,
                             deleteStatus: Int = OK): Unit = {

    // list
    {
      val res =
        route(app, FakeRequest(GET, s"/catalogs/${backendConfig.catalogId.get}/backendConfigs").withUser(user)).get
      status(res) mustBe OK
      contentAsJson(res).as[PagedResponse[BackendConfigReq]].data mustBe (
        if (existsInList) Seq(backendConfig) else Seq.empty)
    }

    // find
    {
      val res = route(
        app,
        FakeRequest(GET, s"/catalogs/${backendConfig.catalogId.get}/backendConfigs/${backendConfig.id.get}")
          .withUser(user)).get
      status(res) mustBe getStatus
      if (getStatus == OK) {
        contentAsJson(res).as[BackendConfigReq] mustBe backendConfig
      }
    }

    // update backendConfig
    {
      val modifiedBackendConfig = backendConfig.copy(backendType = randomStr(10))
      val res = route(
        app,
        FakeRequest(PUT, s"/catalogs/${backendConfig.catalogId.get}/backendConfigs/${backendConfig.id.get}")
          .withUser(user)
          .withJsonBody(Json.toJson(modifiedBackendConfig))
      ).get
      status(res) mustBe updateStatus
      if (updateStatus == OK) {
        contentAsJson(res).as[BackendConfigReq] mustBe modifiedBackendConfig
      }
    }

    // delete backendConfig
    {
      val res = route(
        app,
        FakeRequest(DELETE, s"/catalogs/${backendConfig.catalogId.get}/backendConfigs/${backendConfig.id.get}")
          .withUser(user)).get
      status(res) mustBe deleteStatus
    }
  }

  "Authorization: catalog as an entity" should {

    "CRUDL entity by owner" in {
      val creator = createUser()

      // any principal can create a catalog
      val catalog = createCatalog(creator).right.value

      // without catalogId in realm all operations are forbidden
      checkCatalogRudl(creator, catalog, false, FORBIDDEN, FORBIDDEN, FORBIDDEN)

      val reader = createUser(catalogIds = catalog.id.toSeq)
      // reader has read access only
      checkCatalogRudl(reader, catalog, updateStatus = FORBIDDEN, deleteStatus = FORBIDDEN)

      val admin = creator.copy(catalogIds = catalog.id.toSeq, adminCatalogIds = catalog.id.toSeq)
      // admins have full access
      checkCatalogRudl(admin, catalog)
    }
  }

  "Authorization: catalog scoped entity" should {

    "CRUDL entity as an owner" in {
      val creator = createUser()
      val catalog = createCatalog(creator).right.get
      val owner = creator.copy(catalogIds = catalog.id.toSeq)

      val ds = createDataSource(owner, catalog).right.value
      checkDataSourceRudl(creator, ds, false, FORBIDDEN, FORBIDDEN, FORBIDDEN)

      checkDataSourceRudl(owner, ds)
    }

    "No special permission for tenant admins" in {
      val creator = createUser()
      val catalog = createCatalog(creator).right.get

      val tenantAdmin = createUser(tenantAdmin = true)
      // tenant admins cannot create resources
      createDataSource(tenantAdmin, catalog) mustBe Left(403)

      val owner = creator.copy(catalogIds = catalog.id.toSeq)

      val ds = createDataSource(owner, catalog).right.value
      checkDataSourceRudl(tenantAdmin, ds, false, FORBIDDEN, FORBIDDEN, FORBIDDEN)
    }

    "permission assigned to user works properly" in {
      val creator = createUser()
      val catalog = createCatalog(creator).right.get
      val owner = creator.copy(catalogIds = catalog.id.toSeq)

      val ds = createDataSource(owner, catalog).right.value

      val user = createUser(catalogIds = catalog.id.toSeq)
      checkDataSourceRudl(user, ds, false, FORBIDDEN, FORBIDDEN, FORBIDDEN)

      addPermission(owner, s"/catalogs/${catalog.id.get}/dataSources/${ds.id.get}", user.userId, "Reader")
      checkDataSourceRudl(user, ds, updateStatus = FORBIDDEN, deleteStatus = FORBIDDEN)

      addPermission(owner, s"/catalogs/${catalog.id.get}/dataSources/${ds.id.get}", user.userId, "Editor")
      checkDataSourceRudl(user, ds)
    }

    "permission assigned to group works properly" in {
      val user = createUser()
      val catalog = createCatalog(user).right.get
      val owner = user.copy(catalogIds = catalog.id.toSeq)

      val ds = createDataSource(owner, catalog).right.value

      val groupId = UUID.randomUUID()
      val groupUser = createUser(catalogIds = catalog.id.toSeq, groupIds = Seq(groupId))

      addPermission(owner, s"/catalogs/${catalog.id.get}/dataSources/${ds.id.get}", groupId, "Reader")
      checkDataSourceRudl(groupUser, ds, updateStatus = FORBIDDEN, deleteStatus = FORBIDDEN)

      addPermission(owner, s"/catalogs/${catalog.id.get}/dataSources/${ds.id.get}", groupId, "Editor")
      checkDataSourceRudl(groupUser, ds)
    }
  }

  "Authorization: non-catalog-scoped entities" should {

    "read only" in {
      val user = createUser()
      // user cannot create a jdbcProduct
      createDataSourceProduct(user) mustBe Left(403)

      val prod = createDataSourceProduct(createUser(tenantAdmin = true)).right.get

      // user can read, but not modify an existing jdbcProduct
      checkDataSourceProdRudl(user, prod, updateStatus = FORBIDDEN, deleteStatus = FORBIDDEN)
    }

    "read, modify, create as a tenant admin" in {
      val tenantAdmin = createUser(tenantAdmin = true)
      val prod = createDataSourceProduct(tenantAdmin).right.get

      checkDataSourceProdRudl(tenantAdmin, prod)
    }
  }

  "Authorization: resources in a catalog not explicitly managed (backendConfig)" should {

    "crudl successfully" in {
      val c = createCatalog(createUser())
      val catalog = c.right.get

      val user = createUser()
      // non catalog users cannot create the resource
      createBackendConfig(user, catalog) mustBe Left(403)

      val admin = user.copy(catalogIds = catalog.id.toSeq, adminCatalogIds = catalog.id.toSeq)
      val resource = createBackendConfig(admin, catalog).right.value

      val reader = createUser(catalogIds = catalog.id.toSeq)
      checkBackendConfigRudl(reader, resource, updateStatus = FORBIDDEN, deleteStatus = FORBIDDEN)

      checkBackendConfigRudl(admin, resource)
    }
  }

  "Authorization: as a catalog admin" should {

    "read & modify resources without explicit permission" in {
      val c = createCatalog(createUser())
      val catalog = c.right.get

      val catalogAdmin = createUser(catalogIds = catalog.id.toSeq, adminCatalogIds = catalog.id.toSeq)
      val user = createUser(catalogIds = catalog.id.toSeq)

      val ds = createDataSource(user, catalog).right.value

      checkDataSourceRudl(catalogAdmin, ds)
    }
  }

  "Authorization: permissions crudl" should {
    "crudl successfully" in {
      val catalog = createCatalog(createUser()).right.get
      val owner = createUser(catalogIds = catalog.id.toSeq)
      val ds = createDataSource(owner, catalog).right.get
      val user = createUser(catalogIds = catalog.id.toSeq)

      val dsUrl = s"/catalogs/${catalog.id.get}/dataSources/${ds.id.get}"
      val perm = addPermission(owner, dsUrl, user.userId, "Reader")

      {
        val res = route(app, FakeRequest(GET, s"$dsUrl/permissions").withUser(owner)).get
        status(res) mustBe OK
        contentAsJson(res).as[Seq[PermissionTO]] must contain(perm)
      }

      {
        val res = route(app, FakeRequest(GET, s"$dsUrl/permissions").withUser(user)).get
        status(res) mustBe FORBIDDEN
      }

      {
        val res =
          route(app, FakeRequest(GET, s"$dsUrl/permissions/${perm.id}").withUser(owner)).get
        status(res) mustBe OK
        contentAsJson(res).as[PermissionTO] mustBe perm
      }

      {
        val res = route(app, FakeRequest(GET, s"$dsUrl/permissions/${perm.id}").withUser(user)).get
        status(res) mustBe FORBIDDEN
      }

      {
        val res =
          route(app, FakeRequest(DELETE, s"$dsUrl/permissions/${perm.id}").withUser(user)).get
        status(res) mustBe FORBIDDEN
      }

      {
        val res =
          route(app, FakeRequest(DELETE, s"$dsUrl/permissions/${perm.id}").withUser(owner)).get
        status(res) mustBe OK
      }
    }
  }

}
