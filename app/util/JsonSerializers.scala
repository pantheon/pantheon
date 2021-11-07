package util

import java.net.URI
import java.sql
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.format.DateTimeParseException
import java.util.UUID

import cats.data.NonEmptyList
import play.api.libs.json._
import play.api.libs.json.Writes.UuidWrites
import pantheon.util.{BoolOps, Tap}
import Reads.StringReads
import Writes.StringWrites
import play.api.libs.functional.syntax._
import play.api.libs.functional.~
import enumeratum.Enum
import julienrf.json.derived
import julienrf.json.derived.{DerivedOWrites, DerivedReads}
import shapeless.Lazy

import scala.reflect.ClassTag
import scala.util.control.NonFatal

// Put miscellaneous json serializers here
object JsonSerializers {

  private val adtTypeFormat = (__ \ "type").format[String]

  def adtFormat[T](implicit r: Lazy[DerivedReads[T]], w: Lazy[DerivedOWrites[T]]): OFormat[T] =
    derived.flat.oformat(adtTypeFormat)

  def adtReads[T](implicit r: Lazy[DerivedReads[T]]): Reads[T] =
    derived.flat.reads(adtTypeFormat)

  def adtWrites[T](implicit w: Lazy[DerivedOWrites[T]]): OWrites[T] =
    derived.flat.owrites(adtTypeFormat)

  implicit def nelFormat[T](implicit f: Format[List[T]]): Format[NonEmptyList[T]] = Format(
    f.filter(JsonValidationError("Array cannot be empty"))(_.nonEmpty).map(NonEmptyList.fromListUnsafe),
    nel => f.writes(nel.toList)
  )

  implicit val uriFormat: Format[URI] = Format(
    jsv =>
      jsv.validate[String].flatMap { s =>
        try { JsSuccess(java.net.URI.create(s)) } catch {
          case e: IllegalArgumentException => JsError(s"cannot convert $jsv to URI. Reason:${e.getMessage}")
        }
    },
    uri => JsString(uri.toString)
  )

  implicit val timestampFormat: Format[sql.Timestamp] = Format[sql.Timestamp](
    {
      case JsString(s) =>
        try {
          JsSuccess(sql.Timestamp.valueOf(LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME)))
        } catch {
          case e: DateTimeParseException => JsError(s"Cannot parse timestamp from $s. Reason: ${e.getMessage}")
        }
      case JsNumber(n) =>
        try {
          JsSuccess(new Timestamp(n.toLongExact))
        } catch {
          case e: ArithmeticException => JsError(s"Cannot parse timestamp from $n. Reason: ${e.getMessage}")
        }
      case v => JsError("error.expected.timestamp")
    },
    ts => JsString(ISO_LOCAL_DATE_TIME.format(ts.toLocalDateTime))
  )

  implicit val enumEntryWrites: Writes[enumeratum.EnumEntry] = e => JsString(e.entryName)

  // provides reads for E and each implementation of E separately
  class EnumEntryReadsProvider[E <: enumeratum.EnumEntry](e: Enum[E]) {
    private val enumEntryReads: Reads[E] = _.validate[String].flatMap(n =>
      try {
        JsSuccess(e.withName(n))
      } catch {
        case NonFatal(e) => JsError(e.toString)
    })
    implicit def reads[T <: E: ClassTag]: Reads[T] = enumEntryReads.reads(_).flatMap {
      case t: T => JsSuccess(t)
      case v    => JsError(s"expected ${v.entryName}, got $v")
    }
  }

  // used for cases where it is not possible to add default values to case class definition (like for those in dao.Tables)
  def readsWithRandomDefaultIds[T](r: Reads[T], idFields: String*): Reads[T] = {
    _.validate[JsObject].flatMap(v => r.reads(JsObject(idFields.map(_ -> UuidWrites.writes(UUID.randomUUID()))) ++ v))
  }
  // hack to ensure that optional fields are present even in case of None
  def writesEnsuringFields[A](w: OWrites[A], fieldNames: String*): OWrites[A] = {
    val defaults = JsObject(fieldNames.map(_ -> JsNull)(collection.breakOut))
    w.transform(defaults ++ _)
  }

  def formatWithRenamings[A](f: OFormat[A], renamings: (String, String)*): OFormat[A] =
    OFormat(r = readsWithRenamings(f, renamings: _*), w = writesWithRenamings(f, renamings: _*))

  def readsWithRenamings[A](r: Reads[A], renamings: (String, String)*): Reads[A] = { jv =>
    jv.validate[JsObject]
      .flatMap { obj =>
        val renamedFields =
          renamings.flatMap { case (oldName, newName) => (obj \ newName).asOpt[JsValue].map(oldName -> _) }
        r.reads(new JsObject(obj.value -- renamings.map(_._1) ++ renamedFields))
      }
  }

  def writesWithRenamings[A](w: OWrites[A], renamings: (String, String)*): OWrites[A] = { a =>
    val obj = w.writes(a)
    val renamedFields =
      renamings.flatMap {
        case (oldName, newName) => (obj \ oldName).asOpt[JsValue].map(newName -> _)
      }
    new JsObject(obj.value -- renamings.map(_._1) ++ renamedFields)
  }

  implicit def pairReads[A, B](implicit ar: Reads[A], br: Reads[B]): Reads[A ~ B] = (ar ~ br)(new ~(_, _))
  implicit def pairWrites[A, B](implicit af: OWrites[A], bf: OWrites[B]): OWrites[A ~ B] =
    (af ~ bf).apply(t => t._1 -> t._2)

  implicit def eitherWrites[L, R](implicit left: Writes[L], right: Writes[R]): Writes[Either[L, R]] =
    _.fold(left.writes, right.writes)

}
