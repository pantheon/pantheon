package db

import java.net.URI
import java.util.UUID

import pantheon.schema.Schema
import pantheon.{DataSource, DruidSource, JdbcDataSource, ReflectiveDataSource}
import dao.Tables._
import util.Fixtures
import play.api.test.Helpers._
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.UUID

class DatabaseCatalogSpec extends Fixtures {

  def genId() = UUID.randomUUID()
  def fromDataSource(ds: DataSource): DataSourcesRow = {
    ds match {
      case JdbcDataSource(name, url, props, _, _) =>
        val (dsProd, _) = createDataSourceProduct()
        DataSourcesRow(name, None, props + ("url" -> url), genId(), dsProd.dataSourceProductId, genId())
      case ReflectiveDataSource(name, target) =>
        // TODO: serde for target
        val (dsProd, _) = createDataSourceProduct("reflective")
        DataSourcesRow(name, None, Map[String, String]("target" -> ""), genId(),
          dsProd.dataSourceProductId, genId())
      case DruidSource(name, url, coordinatorUrl, approxDecimal, approxDistinctCount, approxTopN) =>
        val props = Map[String, String](
          "url" -> url.toString,
          "coordinatorUrl" -> coordinatorUrl.toString,
          "approxDecimal" -> approxDecimal.toString,
          "approxDistinctCount" -> approxDistinctCount.toString,
          "approxTopN" -> approxTopN.toString
        )
        val (dsProd, _) = createDataSourceProduct("druid")
        DataSourcesRow(name, None, props, genId(), dsProd.dataSourceProductId, genId())
      case _ =>
        throw new Exception(s"datasource not supported $ds")
    }
  }

  def createCatalog(schema: Schema, sources: Seq[DataSource]) = {
    import profile.api._

    val catRow = CatalogsRow(catalogId = genId())
    val createCat = Catalogs returning Catalogs.map(_.catalogId)

    val schemaRow = SchemasRow(schema.name, None, emptyPsl(schema.name).raw, catRow.catalogId, None, genId())
    val createSchema = Schemas returning Schemas.map(_.schemaId)

    val createDataSource = DataSources returning DataSources.map(_.dataSourceId)

    val rows = sources.map(fromDataSource)

    val actions = for {
      catId <- createCat += catRow
      schemaId <- createSchema += schemaRow.copy(catalogId = catId)
      dsIds <- createDataSource ++= rows.map(row => row.copy(catalogId = catId))
      _ <- DataSourceLinks ++= dsIds.map(DataSourceLinksRow(schemaId, _))
    } yield catId

    val catIdRes = db.run(actions.transactionally)
    await(catIdRes)
  }

  "Catalog" when {

    "fetching a list of data sources" should {
      "be successful" in {
        val schema = Schema("s1")

        val sources = Seq(JdbcDataSource("foodmart", "jdbc:hsqldb:res:foodmart", Map("user" -> "user1",  "schema" -> "foodmart"), None),
                          ReflectiveDataSource("refl", ""))

        val catalogId = createCatalog(schema, sources)

        val fetcher = new DatabaseCatalog(catalogId, components.schemaRepo)
        val res = fetcher.getDatasourcesForSchema(schema.name)

        assert(res.map{
          case s: JdbcDataSource => s.copy(hikariHousekeeperThreadPool = None)
          case v => v
        }.forall(sources.contains(_)))
      }
    }
  }
}
