package services

import dao.Tables._
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.backend.calcite.CalciteBackend
import pantheon.backend.spark.SparkBackend
import util.Fixtures
import play.api.test.Helpers._

class BackendConfigRepoIntegrationSpec extends Fixtures with BaseOneAppPerSuite {

  import profile.api._

  "BackendConfigRepo: positive cases" should {

    "get config from Schema" in {
      val c = createCatalog()
      val s = createSchema(c.catalogId, emptyPsl("test"), Seq.empty)
      val bc1 = createBackendConfig(c.catalogId, "spark")
      val bc2 = createBackendConfig(c.catalogId, "calcite")

      await(
        db.run(Catalogs.filter(_.catalogId === c.catalogId).map(_.backendConfigId).update(Some(bc1.backendConfigId))))
      await(db.run(Schemas.filter(_.schemaId === s.schemaId).map(_.backendConfigId).update(Some(bc2.backendConfigId))))

      await(components.backendConfigRepo.getBackendForSchema(c.catalogId, s.name)) mustBe a[CalciteBackend]
    }

    "get config from Catalog" in {
      val c = createCatalog()
      val s = createSchema(c.catalogId, emptyPsl("test"), Seq.empty)
      val bc = createBackendConfig(c.catalogId, "calcite")

      await(
        db.run(Catalogs.filter(_.catalogId === c.catalogId).map(_.backendConfigId).update(Some(bc.backendConfigId))))

      await(components.backendConfigRepo.getBackendForSchema(c.catalogId, s.name)) mustBe a[CalciteBackend]
    }

    "get empty config" in {
      val c = createCatalog()
      val s = createSchema(c.catalogId, emptyPsl("test"), Seq.empty)
      await(components.backendConfigRepo.getBackendForSchema(c.catalogId, s.name)) mustBe a[CalciteBackend]
    }

  }
}
