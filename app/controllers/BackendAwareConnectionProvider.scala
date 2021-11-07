package controllers

import java.util.UUID

import db.DatabaseCatalog
import pantheon.util.Logging.LoggingContext
import pantheon.{Connection, Pantheon}
import play.api.mvc.Result
import play.api.mvc.Results.NotFound
import services.{BackendConfigRepo, SchemaRepo, |}
import Writables.jsonWritable
import cats.data.EitherT
import dao.Tables.SchemasRow
import cats.instances.future._

import scala.concurrent.{ExecutionContext, Future}

class BackendAwareConnectionProvider(backendConfigRepo: BackendConfigRepo, schemaRepo: SchemaRepo)(
    implicit ec: ExecutionContext) {

  // add-hoc signature suitable for current usecases
  def withConnection[T](catalogId: UUID, schemaId: UUID)(block: Connection => Future[Result | T])(
      implicit ctx: LoggingContext): EitherT[Future, Result, T] =
    for {
      schema <- EitherT.fromOptionF[Future, Result, SchemasRow](
        schemaRepo.find(catalogId, schemaId),
        NotFound(ActionError(s"""schema "$schemaId" not found"""))
      )
      backend <- EitherT.liftF(backendConfigRepo.getBackendForSchema(catalogId, schema.name))
      catalog = new DatabaseCatalog(catalogId, schemaRepo)
      res <- EitherT(Pantheon.withConnection(catalog, schema.name, backend)(block(_)))
    } yield res
}
