package util

import java.io.File
import java.net.URI
import java.util.UUID

import dao.Tables._
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.play.{FakeApplicationFactory, PlaySpec}
import play.api.{Application, ApplicationLoader, Configuration, Environment}
import play.api.inject.DefaultApplicationLifecycle
import play.api.test.Helpers._
import play.core.DefaultWebCommands
import slick.jdbc.PostgresProfile.api._
import DBIOUtil.DBIOOps
import akka.util.Timeout
import com.zaxxer.hikari.HikariConfig
import config.Authentication.User
import config.{Authentication, PantheonAppLoader, PantheonComponents}
import controllers.NullableWritesOptionWrapper
import controllers.helpers.CrudlActions.{Page, PagedResponse}
import dao.Tables.{CatalogsRow, DataSourcesRow, SchemasRow}
import messagebus.SingleNodeMessageBus
import pdi.jwt.JwtJson
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.mvc.Result
import play.api.test.FakeRequest
import services.CatalogRepo.CatalogId
import services.DataSourceProductRepo.DataSourceProductProperty
import services.SchemaRepo.{ParsedPsl, SchemaId}
import slick.jdbc.JdbcBackend

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object Fixtures {
  implicit val pageReads = Json.reads[Page]
  implicit def pagedResponseReads[T: Reads]: Reads[PagedResponse[T]] = (
    (JsPath \ "page").read[Page] and
      (JsPath \ "data").read[Seq[T]]
    )((p, d) => new PagedResponse[T] {
    override val page: Page = p
    override val data: Seq[T] = d
  })
}

abstract class Fixtures extends PlaySpec with FakeApplicationFactory with BeforeAndAfterEach {

  import Fixtures.pagedResponseReads

  val env = Environment.simple(new File("."))
  lazy val appContext = ApplicationLoader.Context(
    environment = env,
    sourceMapper = None,
    webCommands = new DefaultWebCommands(),
    initialConfiguration = Configuration.load(env),
    lifecycle = new DefaultApplicationLifecycle()
  )

  def emptyPsl(name: String) =
    ParsedPsl(s"schema $name")
      .ensuring(_.isRight, "Failed to parse schema")
      .right
      .get

  def parsePsl(psl: String) =
    ParsedPsl(psl).fold(e => throw new AssertionError("Failed to parse schema:" + e), identity)

  // Warning: will not recreate components in case of OneAppPerTest
  lazy val components = new PantheonComponents(appContext, (as, _, mp) => new SingleNodeMessageBus(mp, as.dispatcher))

  def db: JdbcBackend#DatabaseDef = components.dbConfig.db

  override def fakeApplication: Application = new PantheonAppLoader(_ => components, false).load(appContext)

  def truncate(tables: String*)= {
    val tablesClause = tables.mkString(",")
    await(db.run(sqlu"TRUNCATE TABLE #$tablesClause CASCADE"))(Timeout(1.minute))
  }

  def cleanUpDynamicProducts() = {

    val productsTables = Seq(
      "data_source_product_properties",
      "data_source_product_icons",
      "data_source_product_jars",
      "data_source_products"
    )
    val idColumn = "data_source_product_id"

    val bundledIds = await(
      db.run(DataSourceProducts.filter(_.isBundled).map(_.dataSourceProductId).result)
    )(Timeout(1.minute)).map(v => s"'$v'").mkString(",")

    if(bundledIds.nonEmpty) {
      productsTables.foreach(table =>
        await(db.run(sqlu"DELETE FROM #$table where #$idColumn NOT IN(#$bundledIds)"))(Timeout(1.minute)))
    }
  }

  def cleanUpDb():Unit = {

      truncate("catalogs",
        "schemas",
        "schema_usages",
        "data_sources",
        "data_source_links",
        "backend_configs",
        "query_history",
        "tbls",
        "cols",
        "principal_permissions"
      )

    cleanUpDynamicProducts()
  }
  override def beforeEach() = cleanUpDb()

  def createCatalog(): CatalogsRow = await(
    Catalogs.returning(Catalogs).+=(CatalogsRow(Some(randomStr(10)), Some(randomStr(10)), UUID.randomUUID())).run(db)
  )

  def generateDataSourceProductRow(typ: String) =
    DataSourceProductsRow(
      false,
      randomStr(10),
      None,
      UUID.randomUUID(),
      randomStr(10),
      Some(randomStr(10)),
      typ)

  def generateDataSourceProductProps() =
    Seq(
      DataSourceProductProperty(randomStr(10), randomStr(10)),
      DataSourceProductProperty(randomStr(10), randomStr(10)),
      DataSourceProductProperty(randomStr(10), randomStr(10))
    )

  def createDataSourceProduct(typ: String = "jdbc"): (DataSourceProductsRow, Seq[DataSourceProductProperty]) =
    createDataSourceProduct(generateDataSourceProductRow(typ), generateDataSourceProductProps())

  def createDataSourceProduct(
    prod: DataSourceProductsRow,
    props: Seq[DataSourceProductProperty]): (
    DataSourceProductsRow, Seq[DataSourceProductProperty]) = {

    val action = for {
      savedProduct <- DataSourceProducts.returning(DataSourceProducts) += prod
      _ <- DataSourceProductProperties ++= props.zipWithIndex.map {
        case (p, i) => DataSourceProductPropertiesRow(
          UUID.randomUUID(), savedProduct.dataSourceProductId, p.name, p.`type`, i)
      }
    } yield (savedProduct, props)

    await(db.run(action))
  }

  def generateBackendConfigRow(catalogId: UUID, backendType: String = "calcite") =
    BackendConfigsRow(Some(randomStr(10)),
                      backendType,
                      Some(randomStr(10)),
                      Map(randomStr(10) -> randomStr(10)),
                      catalogId,
                      UUID.randomUUID())

  def createBackendConfig(catalogId: UUID, backendType: String = "calcite"): BackendConfigsRow =
    await(
      db.run(
        BackendConfigs.returning(BackendConfigs) += generateBackendConfigRow(catalogId, backendType)
      )
    )

  def createSchema(
      catalogId: UUID,
      psl: ParsedPsl,
      dataSourceIds: Seq[UUID]
  ): SchemasRow = {

    val insert = Schemas returning Schemas.map(_.schemaId)
    val row = SchemasRow(psl.parsed.name, None, psl = psl.raw, catalogId, None, UUID.randomUUID())

    val actions = (for {
      id <- insert += row
      _ <- DataSourceLinks ++= dataSourceIds.map(dsId => DataSourceLinksRow(id, dsId))
      res <- Schemas.withFilter(_.schemaId === id).result.head
    } yield res).transactionally

    await(db.run(actions))(100.seconds)
  }

  def createDataSource(catalogId: UUID,
                       name: String,
                       properties: Map[String, String],
                       dataSourceProductId: Option[UUID] = None): DataSourcesRow = {
    val dsProductId = dataSourceProductId.getOrElse(createDataSourceProduct()._1.dataSourceProductId)
    val insert = DataSources returning DataSources.map(_.dataSourceId)
    val row =
      DataSourcesRow(name,
                     properties = properties,
                     dataSourceProductId = dsProductId,
                     dataSourceId = UUID.randomUUID(),
                     catalogId = catalogId)

    val actions = (for {
      id <- insert += row
      res <- DataSources.withFilter(_.dataSourceId === id).result.head
    } yield res).transactionally

    await(db.run(actions))
  }

  def createSavedQuery(query: SavedQueriesRow): SavedQueriesRow = {

    val insert = SavedQueries returning SavedQueries.map(_.savedQueryId)

    val actions = (for {
      id <- insert += query
      res <- SavedQueries.withFilter(_.savedQueryId === id).result.head
    } yield res).transactionally

    await(db.run(actions))
  }

  def randomStr(n: Int): String = {
    Random.alphanumeric.take(n).mkString
  }

  def createFoodmartDS(catalogId: UUID, name: String = "foodmart"): DataSourcesRow = {
    val dsParams = Map[String, String](
      "url" -> "jdbc:hsqldb:res:foodmart",
      "user" -> "FOODMART",
      "password" -> "FOODMART",
      "schema" -> "foodmart"
    )

    // a product id should exist, otherwise something went wrong
    val dsProdId = await(
      db.run(DataSourceProducts.filter(_.`type` === "hsqldb-foodmart").map(_.dataSourceProductId).take(1).result)
    )(Timeout(1.minute)).head
    createDataSource(catalogId, name, dsParams, Some(dsProdId))
  }

  def createFoodMartBasedSchemaReturningAll(name: String, psl: String): (CatalogsRow, DataSourcesRow, SchemasRow) = {
    val catalog = createCatalog()
    val ds = createFoodmartDS(catalog.catalogId)
    val schema = createSchema(catalog.catalogId, parsePsl(psl), Seq(ds.dataSourceId))
    (catalog, ds, schema)
  }
  // Returns catalog id and schema id
  def createFoodMartBasedSchema(name: String, psl: String): (CatalogId, SchemaId) = {
    val r = createFoodMartBasedSchemaReturningAll(name, psl)
    r._1.catalogId -> r._3.schemaId
  }

  def provideSchema(): (CatalogId, SchemaId) = provideSchemaWithImports()._1

  def provideSchemaWithImports(importSchemas: ParsedPsl*): ((CatalogId, SchemaId), Seq[SchemaId]) = {

    val cid = createCatalog().catalogId
    val did = createFoodmartDS(cid).dataSourceId

    val importsBlock =
      if (importSchemas.isEmpty) ""
      else importSchemas.map(_.parsed.name).mkString("import{", "\n,", "\n}")

    val foodmart =
      s"""schema Foodmart (dataSource = "foodmart", strict = false) {
         |
         |  $importsBlock
         |
         |  measure unitSales(column = "sales_fact_1998.unit_sales") {
         |    metadata(name = "Unit Sales", description = "My unit sales",
         |      tags = ["a", "b", "c"], active = true, weight = 3.14)
         |  }
         |
         |  measure storeSales(column = "sales_fact_1998.store_sales") {
         |    metadata(name = "Store Sales", description = "My store sales")
         |  }
         |
         |  dimension Customer(table = "customer") {
         |    attribute lastName(column = "lname")
         |    attribute gender
         |  }
         |
         |  dimension Store(table = "store") {
         |    attribute name(column = "store_name")
         |    attribute city(column = "store_city")
         |    attribute state(column = "store_state")
         |    attribute number(column = "store_number")
         |    attribute coffeeBar(column = "coffee_bar")
         |  }
         |
         |  table sales_fact_1998 {
         |    column customer_id (tableRef = "customer.customer_id")
         |    column store_id (tableRef = "store.store_id")
         |    column unit_sales
         |    column store_sales
         |  }
         |}
     """.stripMargin

    cid ->
      createSchema(cid, ParsedPsl(foodmart).right.get, Seq(did)).schemaId ->
      importSchemas.map(createSchema(cid, _, Seq(did)).schemaId)
  }

  implicit def nullableOptReads[T](implicit r: Reads[T]): Reads[NullableWritesOptionWrapper[T]] =
    r.map(v => NullableWritesOptionWrapper(Some(v))) orElse Reads.pure(NullableWritesOptionWrapper.none)

  def createUser(tenantAdmin: Boolean = false,
                 catalogIds: Seq[UUID] = Seq.empty,
                 adminCatalogIds: Seq[UUID] = Seq.empty,
                 groupIds: Seq[UUID] = Seq.empty): User =
    User(UUID.randomUUID(), randomStr(10), tenantAdmin, catalogIds, adminCatalogIds, groupIds)

  implicit class AuthFakeRequest[A](req: FakeRequest[A]) {
    def withUser(user: User): FakeRequest[A] = {
      val claim = Json.toJsObject(user)(Authentication.userWrites)
      req.withHeaders("X-Request-Token" -> JwtJson.encode(claim))
    }
  }

  def testPagination[T](app: Application,
                        cid: Option[UUID],
                        relativePath: String,
                        createResource: Int => Unit,
                        cleanupBeforeNoDataTest: () => Unit,
                        numOfPersistentElementsOpt: Option[Int] = None)(implicit rd: Reads[T]) = {
    import org.scalatest.prop.TableDrivenPropertyChecks._

    (1 to 10).foreach(createResource)

    def mkCatPath(cid: UUID) = s"/catalogs/$cid"
    val catPath = cid.fold("")(mkCatPath)
    val numOfPersistentElements = numOfPersistentElementsOpt.getOrElse(0)

    {
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath")).get
      val pr = contentAsJson(list).as[PagedResponse[T]]
      pr.page mustBe Page(1, Int.MaxValue, 10 + numOfPersistentElements)
      pr.data.size mustBe 10 + numOfPersistentElements
    }
    {
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=1&pageSize=3")).get
      val pr = contentAsJson(list).as[PagedResponse[T]]
      pr.page mustBe Page(1, 3, 10 + numOfPersistentElements)
      pr.data.size mustBe 3
    }
    {
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=4&pageSize=3")).get
      val pr = contentAsJson(list).as[PagedResponse[T]]
      pr.page mustBe Page(4, 3, 10 + numOfPersistentElements)
      pr.data.size mustBe Math.min(numOfPersistentElements + 1, 3)

    }

    { //no data to show in page
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=999&pageSize=3")).get
      val pr = contentAsJson(list).as[PagedResponse[T]]
      pr.page mustBe Page(999, 3, 10 +numOfPersistentElements)
      pr.data.size mustBe 0
    }

    if (catPath.nonEmpty) {
      //catalog does not exist.
      val nonExistent = UUID.randomUUID()
      val list = route(app, FakeRequest(GET, s"${mkCatPath(nonExistent)}/$relativePath?page=1&pageSize=3")).get

      contentAsJson(list) mustBe Json.parse(s"""{"errors":["catalog '$nonExistent' not found"]}""")
      status(list) mustBe NOT_FOUND
    }

    { //no data
      cleanupBeforeNoDataTest()
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=1&pageSize=3")).get

      status(list) mustBe OK
      val pr = contentAsJson(list).as[PagedResponse[T]]
      pr.page mustBe Page(1, 3, numOfPersistentElements)
      pr.data.size mustBe Math.min(numOfPersistentElements, 3)
    }

    //negative tests
    {
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?pageSize=3")).get
      contentAsJson(list) mustBe Json.parse("""{"errors":["page and pageSize must be defined together"]}""")
    }
    {
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=1")).get
      contentAsJson(list) mustBe Json.parse("""{"errors":["page and pageSize must be defined together"]}""")
    }
    forAll(Table("page" -> "pageSize", (0, -1), (0, 3), (1, 0), (-1, 1), (1, -1))) { (p, ps) =>
      val list = route(app, FakeRequest(GET, s"$catPath/$relativePath?page=$p&pageSize=$ps")).get
      contentAsJson(list) mustBe Json.parse("""{"errors":["page and pageSize must be positive"]}""")
    }
  }

  def testLeak(block : => Unit): Unit =
    for (_ <- 0 until new HikariConfig().getMaximumPoolSize * 2) {
      block
    }
}
