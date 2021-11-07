package pantheon.schema.parser

sealed trait AntlrException extends RuntimeException

case class SyntaxError(line: Int, charPos: Int, override val getMessage: String) extends AntlrException
