package db

import java.util.UUID

import pantheon.{Catalog, DataSource}
import pantheon.schema.Schema

import scala.language.implicitConversions
import scala.concurrent.Await
import scala.concurrent.duration._
import services.SchemaRepo

class DatabaseCatalog(catalogId: UUID, schemaRepo: SchemaRepo) extends Catalog {

  def getSchema(name: String): Option[Schema] = {
    val res = schemaRepo.findAndCompile(catalogId, name)
    Await.result(res, 10.second)
  }

  def getDatasourcesForSchema(name: String): Seq[DataSource] = {
    val res = schemaRepo.getDataSourcesForSchemaName(catalogId, name)
    Await.result(res, 10.second)
  }
}
