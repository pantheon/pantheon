package pantheon.errors

sealed trait QueryException {
  val msg: String
}

class InvalidQueryStructureException(val msg: String) extends Exception(msg) with QueryException
class IncompatibleQueryException(val msg: String, schemaName: String) extends Exception(msg) with QueryException
