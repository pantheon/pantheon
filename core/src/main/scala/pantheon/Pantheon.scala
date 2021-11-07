package pantheon

import pantheon.backend.Backend
import pantheon.backend.calcite.CalciteBackend
import pantheon.util.Logging.LoggingContext
import pantheon.util.withResourceManual

object Pantheon {
  def withConnection[T](catalog: Catalog, schemaName: String, backend: Backend = CalciteBackend())(
      block: Connection => T)(implicit ctx: LoggingContext): T =
    withResourceManual(new Connection(catalog, schemaName, backend))(_.close(ctx))(block)
}
