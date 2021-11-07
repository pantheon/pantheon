package pantheon.backend.spark

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.DataFrame
import pantheon._
import org.apache.spark.sql.types
import org.apache.spark.sql.types.DataTypes

import scala.collection.AbstractIterator

object SparkRowSet {

  def convertDataType(dataType: types.DataType): DataType = dataType match {
    case DataTypes.BooleanType => BooleanType
    case DataTypes.ByteType    => ByteType
    case DataTypes.DateType    => DateType
    // We ignore decimal scale coming from Spark right now.
    case _: types.DecimalType    => DecimalType(-1)
    case DataTypes.DoubleType    => DoubleType
    case DataTypes.FloatType     => FloatType
    case DataTypes.IntegerType   => IntegerType
    case DataTypes.LongType      => LongType
    case DataTypes.NullType      => NullType
    case _: types.ObjectType     => ObjectType
    case DataTypes.ShortType     => ShortType
    case DataTypes.StringType    => StringType
    case DataTypes.TimestampType => TimestampType
  }

  case class SparkFieldDescriptor(name: String, dataType: DataType, nullable: Boolean) extends FieldDescriptor

  // An adapter for the Spark result set row
  // Made in such way to avoid allocating additional objects for each row
  class SparkRow extends Row {
    private var currRow: org.apache.spark.sql.Row = _

    def withRow(row: org.apache.spark.sql.Row): SparkRow = {
      currRow = row
      this
    }

    override def isNull(i: Int): Boolean = currRow.isNullAt(i)

    override def apply(i: Int): Any = currRow.apply(i)

    override def getString(i: Int): String = currRow.getString(i)

    override def getBoolean(i: Int): Boolean = currRow.getBoolean(i)

    override def getByte(i: Int): Byte = currRow.getByte(i)

    override def getDate(i: Int): Date = currRow.getDate(i)

    override def getDecimal(i: Int): BigDecimal = currRow.getDecimal(i)

    override def getDouble(i: Int): Double = currRow.getDouble(i)

    override def getFloat(i: Int): Float = currRow.getFloat(i)

    override def getInteger(i: Int): Int = currRow.getInt(i)

    override def getLong(i: Int): Long = currRow.getLong(i)

    override def getObject(i: Int): Any = currRow.apply(i)

    override def getShort(i: Int): Short = currRow.getShort(i)

    override def getTimestamp(i: Int): Timestamp = currRow.getTimestamp(i)
  }
}

class SparkRowSet(dataFrame: DataFrame, preAction: () => Unit) extends RowSet {
  import SparkRowSet._

  override def fields: Seq[FieldDescriptor] = {
    preAction()
    dataFrame.schema.map(f => SparkFieldDescriptor(f.name, convertDataType(f.dataType), f.nullable))
  }

  override def rows: Iterator[Row] = new AbstractIterator[Row] {
    preAction()
    private val dfIter = dataFrame.toLocalIterator()
    private val row = new SparkRow

    override def hasNext: Boolean = {
      preAction()
      dfIter.hasNext
    }
    override def next(): Row = {
      preAction()
      row.withRow(dfIter.next())
    }
  }

  override def close(): Unit = {}
}
