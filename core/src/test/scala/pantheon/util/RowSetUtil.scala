package pantheon.util

import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import pantheon._

import scala.math.BigDecimal.RoundingMode

object RowSetUtil {
  sealed trait TimestampFormat
  case object Date extends TimestampFormat
  case object DateTime extends TimestampFormat

  def toStringList(
      rs: RowSet,
      limit: Int = 100,
      zoneId: Option[ZoneId] = None,
      decimalScale: Int = 3,
      timestampFormat: TimestampFormat = DateTime
  ): List[String] = {
    val formatDate = new SimpleDateFormat("yyyy-MM-dd")
    val formatDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val meta = rs.fields
    val res = rs.rows
      .take(limit)
      .map { rs =>
        meta.zipWithIndex
          .map {
            case (fd, i) =>
              // TODO: how to avoid code duplication between 'withNullCheck' and 'toJson' from QueryUtils.getRow
              def withNullCheck[T](v: Int => T)(f: T => String): String =
                if (rs.isNull(i))
                  if (fd.nullable) null
                  else throw new AssertionError(s"got null for non-nullable field ${fd.name}")
                else f(v(i))

              def formatDecimal[T](get: Int => BigDecimal, whole: Boolean): String = {
                if (rs.isNull(i)) null
                else {
                  // Oracle doesn't have separate INTEGER/FLOAT, both types are
                  // mapped into NUMBER(precision, scale), which maps into
                  // java.sql.BigDecimal.
                  //
                  // So, we need to extract 'scale' from type definition in DB
                  // in order to format it correctly as integer in case when
                  // 'scale' is zero.
                  //
                  // https://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm#sthref106
                  if (whole) {
                    f"${get(i)}%1.0f"
                  } else {
                    // double rounding to avoid rounding errors
                    val v = get(i).setScale(decimalScale, RoundingMode.HALF_UP)
                    f"$v%1.3f"
                  }
                }
              }

              val v = fd.dataType match {
                case DateType =>
                  withNullCheck(rs.getDate)(formatDate.format)

                case TimestampType =>
                  // Some JDBC like Oracle maps DATE type to same
                  // java.sql.Timestamp, so we need to specify when we are
                  // expecting DATE instead of DATETIME/TIMESTAMP.
                  //
                  // https://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm#sthref106
                  if (timestampFormat == Date) {
                    withNullCheck(rs.getTimestamp)(formatDate.format)
                  } else {
                    val zone = zoneId.getOrElse(ZoneId.systemDefault())
                    withNullCheck(rs.getTimestamp)(ts =>
                      formatDateTime.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime), zone)))
                  }

                case FloatType | DoubleType =>
                  formatDecimal(i => BigDecimal(rs.getDouble(i)), false)

                case decimal: DecimalType =>
                  formatDecimal(i => rs.getDecimal(i), decimal.scale == 0)

                case IntegerType =>
                  withNullCheck(rs.getInteger)(_.toString)

                case LongType =>
                  withNullCheck(rs.getLong)(_.toString)

                case ShortType =>
                  withNullCheck(rs.getShort)(_.toString)

                case _ => rs(i)
              }
              fd.name + "=" + v
          }
          .mkString("; ")
      }
      .toList
    rs.close()
    res
  }

  def toString(
      rs: RowSet,
      limit: Int = 100,
      decimalScale: Int = 3,
      timestampFormat: TimestampFormat = DateTime
  ): String =
    toStringList(rs, limit, None, decimalScale, timestampFormat).mkString("\n")
}
