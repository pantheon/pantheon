package util

import cats.Monad
import slick.basic.BasicBackend
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import scala.concurrent.{ExecutionContext, Future}

object DBIOUtil {

  object dbio {
    def cond[R, S <: NoStream, E <: Effect](b: Boolean,
                                            a: => DBIOAction[R, S, E],
                                            default: => R): DBIOAction[R, NoStream, E] =
      if (b) a else DBIO.successful(default)

    def cond[R, S <: NoStream, E <: Effect](b: Boolean, a: => DBIOAction[R, S, E])(
        implicit ec: ExecutionContext): DBIOAction[Unit, NoStream, E] =
      if (b) a.map(_ => ()) else DBIO.successful(())

  }

  implicit class DBIOOps[R, S <: NoStream, E <: Effect](val a: DBIOAction[R, S, E]) extends AnyVal {
    def run[P](dbd: BasicBackend#DatabaseDef): Future[R] = dbd.run(a)
  }
  // flexible on type parameters  (we have monad instance only for DBIO[T])
  implicit class OptionOps[T](val o: Option[T]) extends AnyVal {
    def traverseDBIO[R, S <: NoStream, E <: Effect](f: T => DBIOAction[R, S, E]): DBIOAction[Option[R], NoStream, E] =
      DBIO.sequenceOption(o.map(f))

    def sequenceDBIO[R, S <: NoStream, E <: Effect](
        implicit ev: T <:< DBIOAction[R, S, E]): DBIOAction[Option[R], NoStream, E] =
      DBIO.sequenceOption(o.map(ev(_)))
  }

  // flexible on type parameters
  implicit class EitherOps[L, R](val o: Either[L, R]) extends AnyVal {
    def traverseDBIO[R1, S <: NoStream, E <: Effect](f: R => DBIOAction[R1, S, E])(
        implicit ec: ExecutionContext): DBIOAction[Either[L, R1], NoStream, E] =
      o.fold(l => DBIO.successful(Left(l)), f(_).map(Right(_)))

    def sequenceDBIO[R1, S <: NoStream, E <: Effect](implicit ev: R <:< DBIOAction[R1, S, E],
                                                     ec: ExecutionContext): DBIOAction[Either[L, R1], NoStream, E] =
      o.traverseDBIO(identity(_))
  }

  implicit def dbioMonad(implicit ec: ExecutionContext): Monad[DBIO] = new Monad[DBIO] {
    override def pure[A](x: A) = DBIO.successful(x)
    override def flatMap[A, B](fa: DBIO[A])(f: A => DBIO[B]) = fa.flatMap(f)
    override def tailRecM[A, B](a: A)(f: A => DBIO[Either[A, B]]): DBIO[B] = f(a).flatMap(_.fold(tailRecM(_)(f), pure))

  }

}
