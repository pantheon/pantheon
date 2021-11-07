package errors

sealed trait PantheonError

sealed trait RepoError extends PantheonError
case class ConcurrentModification(msg: String) extends RepoError
case class ConstraintViolation(error: String) extends RepoError

case object NotAuthorized extends PantheonError
