package pantheon

import pantheon.schema.Schema

trait Catalog {

  def getSchema(name: String): Option[Schema]

  def getDatasourcesForSchema(name: String): Seq[DataSource]
}
