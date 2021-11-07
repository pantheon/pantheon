package pantheon
import java.net.URI
import java.util.concurrent.ScheduledExecutorService

import pantheon.schema.{Schema, SchemaCompiler}
import pantheon.schema.parser.SchemaParser
import cats.syntax.either._
import cats.syntax.traverse._
import cats.instances.list._

object InMemoryCatalog {

  case class DataSourceDef(name: String, typ: String, params: Map[String, String])
  def create(schemas: Map[String, String],
             defs: List[DataSourceDef],
             hikariHousekeeperThreadPool: Option[ScheduledExecutorService] = None): Either[String, InMemoryCatalog] =
    defs
      .traverse(d => DataSource.create(d.name, d.typ, d.params, hikariHousekeeperThreadPool).toValidatedNel)
      .map(new InMemoryCatalog(schemas, _))
      .toEither
      .leftMap(_.toList.mkString(";"))
}
class InMemoryCatalog(schemas: Map[String, String], dataSources: List[DataSource]) extends Catalog {

  override def getSchema(name: String): Option[Schema] = {
    schemas
      .get(name)
      .map(
        SchemaParser(_)
          .flatMap(SchemaCompiler.compile(this, _).left.map(_.map(_.message).mkString(",")))
          .valueOr(err => throw new Exception("Schema parsing or compilation was unsuccessful: " + err))
      )
  }

  override def getDatasourcesForSchema(name: String): Seq[DataSource] = dataSources
}
