import errors.{ConstraintViolation, PantheonError, RepoError}
import org.postgresql.util.PSQLException

import scala.concurrent.{ExecutionContext, Future}

package object services {

  type |[A, B] = Either[A, B]

  def show(s: Iterable[String]) = s.mkString("[", ",", "]")

  implicit class ResponseOps[R](val r: Future[RepoError | R]) extends AnyVal {
    // detects constraintViolation and lets user wrap it into business level error
    def detectConstraintViolation(mkErr: String => RepoError = ConstraintViolation(_))(
        implicit ec: ExecutionContext): Future[RepoError | R] = {
      r.recover {
        case e: PSQLException if e.getMessage.contains("constraint") =>
          println(e.getMessage)
          Left(mkErr(e.getMessage.stripPrefix("ERROR: ").replaceAll("\n", ";")))
      }
    }
  }

}
