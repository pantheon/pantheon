package errors

case class ValidationError(fieldName: String, errors: List[String])
