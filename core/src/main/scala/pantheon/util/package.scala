package pantheon

import java.io.{PrintWriter, StringWriter}

import cats.Functor

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

package object util {
  // TODO: there may be different approaches to generalize this to multiple resources
  def withResource[R <: AutoCloseable, T](r: R)(f: R => T): T = {
    try { f(r) } finally { r.close }
  }

  def withResourceManual[R, T](r: R)(close: R => Unit)(f: R => T): T = {
    try { f(r) } finally { close(r) }
  }

  // taken from shapeless(https://github.com/milessabin/shapeless/blob/master/core/src/main/scala/shapeless/package.scala)
  trait =:!=[A, B] extends Serializable
  object =:!= {
    implicit def neq[A, B]: A =:!= B = new =:!=[A, B] {}
    implicit def neqAmbig1[A]: A =:!= A = ???
    implicit def neqAmbig2[A]: A =:!= A = ???
  }

  implicit class BoolOps(val b: Boolean) extends AnyVal {
    def option[A](a: => A): Option[A] = if (b) Some(a) else None
    def either[A](a: => A): Either[A, A] = if (b) Right(a) else Left(a)
  }

  implicit class Tap[A](val a: A) extends AnyVal {
    def tap(f: (A) => Unit): A = {
      f(a)
      a
    }

    def |>[T](f: A => T): T = f(a)

    def ?|>[T](f: A => T): Option[T] = nullSafeTap(f)
    def ?|>>[T](f: A => Option[T]): Option[T] = Option(a).flatMap(f)

    def nullSafeTap[T](f: A => T): Option[T] = Option(a).map(f)
  }

  def getStackTrace(e: Throwable): String = {
    withResource(new StringWriter()) { sWriter =>
      withResource(new PrintWriter(sWriter)) { pWriter =>
        e.printStackTrace(pWriter)
        pWriter.flush()
        sWriter.toString
      }
    }
  }

  def duplicates[C[x] <: Seq[x], T, K](a: C[T]) = a.diff(a.distinct).distinct

  def joinUsing[C[x] <: Iterable[x], C1[x] <: Iterable[x], T, K, R](a: C[T], b: C1[T])(key: T => K)(
      f: (T, Iterable[T]) => R)(implicit bf: CanBuildFrom[Nothing, R, C[R]]): C[R] = join2Using(a, b)(key, key)(f)

  def join2Using[C[x] <: Iterable[x], C1[x] <: Iterable[x], T1, T2, K, R](a: C[T1], b: C1[T2])(
      key1: T1 => K,
      key2: T2 => K)(f: (T1, Iterable[T2]) => R)(implicit bf: CanBuildFrom[Nothing, R, C[R]]): C[R] = {
    val hash: Map[K, Iterable[T2]] = b.groupBy(key2)
    a.map { v =>
      f(v, hash.getOrElse(key1(v), Nil))
    }(collection.breakOut)
  }

  def mapWitIndex[C[_], T, R](s: C[T])(f: (Int, T) => R)(implicit fc: Functor[C]): C[R] = {
    var i: Int = -1
    fc.map(s) {
      i = i + 1
      f(i, _)
    }
  }

  // this function is curried to improve type inference of C
  def toGroupedMap[C[x] <: TraversableLike[x, C[x]], A, B, R](conformPairs: C[(A, B)])(combine: C[B] => R)(
      implicit cbf: CanBuildFrom[C[(A, B)], B, C[B]]): Map[A, R] =
    conformPairs
      .groupBy(_._1)
      .mapValues(v => combine(v.map(_._2)))

  /**
    * ensures that type C represents one of the children of P.
    * phantom type(no instances required)
    */
  @annotation.implicitNotFound(msg = "${C} is not a child of sealed trait ${P}")
  class ConcreteChild[C, P]
  object ConcreteChild {
    implicit def sc[P, C <: P](
        implicit ev: C =:!= P,
        ev1: C =:!= P with Product with Serializable
    ): ConcreteChild[C, P] = null
  }
}
