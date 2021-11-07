package pantheon.backend

import pantheon.schema.Schema

trait Backend {
  def getConnection(schema: Schema): BackendConnection
}
