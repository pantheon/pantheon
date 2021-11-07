package pantheon

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.{Instant, ZonedDateTime}
import java.time.format.DateTimeFormatter

sealed trait DataType
case object BooleanType extends DataType
case object ByteType extends DataType
case object DateType extends DataType
case object DoubleType extends DataType
case object FloatType extends DataType
case object IntegerType extends DataType
case object LongType extends DataType
case object NullType extends DataType
case object ObjectType extends DataType
case object ShortType extends DataType
case object StringType extends DataType
case object TimestampType extends DataType

// scale is number of points right of decimal point.
// scale = 0 means that number is integer.
case class DecimalType(scale: Int) extends DataType

// Discuss: make this case class?
trait FieldDescriptor {
  def name: String
  def dataType: DataType
  def nullable: Boolean
}

trait Row {
  def isNull(i: Int): Boolean
  def apply(i: Int): Any
  def getBoolean(i: Int): Boolean
  def getByte(i: Int): Byte
  def getDate(i: Int): Date
  def getDecimal(i: Int): BigDecimal
  def getDouble(i: Int): Double
  def getFloat(i: Int): Float
  def getInteger(i: Int): Int
  def getLong(i: Int): Long
  def getObject(i: Int): Any
  def getShort(i: Int): Short
  def getString(i: Int): String
  def getTimestamp(i: Int): Timestamp
}

trait RowSet extends AutoCloseable {
  def fields: Seq[FieldDescriptor]
  def rows: Iterator[Row]
}
