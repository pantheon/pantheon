package pantheon.backend.calcite

import java.sql._

import pantheon._

object JdbcRowSet {

  def convertDataType(sqlDataType: Int, precision: Int, scale: Int): DataType = sqlDataType match {
    case Types.BOOLEAN => BooleanType
    case Types.BIT     =>
      // Mysql commonly uses bit(1) for booleans,
      // the Postgres driver reports booleans as type Bit for backwards compatibility
      // https://github.com/pgjdbc/pgjdbc/issues/367
      if (precision == 1) BooleanType else ByteType
    case Types.TINYINT              => ByteType
    case Types.DATE                 => DateType
    case Types.DECIMAL              => DecimalType(scale)
    case Types.DOUBLE               => DoubleType
    case Types.FLOAT                => FloatType
    case Types.INTEGER              => IntegerType
    case Types.BIGINT               => LongType
    case Types.NULL                 => NullType
    case Types.SMALLINT             => ShortType
    case Types.CHAR | Types.VARCHAR => StringType
    case Types.TIMESTAMP            => TimestampType
    case _                          => ObjectType
  }

  class JdbcRowIterator(val resultSet: ResultSet) extends Iterator[Row] {
    private val row = new Row {

      override def isNull(i: Int): Boolean = {
        getObject(i)
        resultSet.wasNull()
      }

      override def apply(i: Int): Any = resultSet.getObject(i + 1)

      override def getString(i: Int): String = resultSet.getString(i + 1)

      override def getBoolean(i: Int): Boolean = resultSet.getBoolean(i + 1)

      override def getByte(i: Int): Byte = resultSet.getByte(i + 1)

      override def getDate(i: Int): Date = resultSet.getDate(i + 1)

      override def getDecimal(i: Int): BigDecimal = resultSet.getBigDecimal(i + 1)

      override def getDouble(i: Int): Double = resultSet.getDouble(i + 1)

      override def getFloat(i: Int): Float = resultSet.getFloat(i + 1)

      override def getInteger(i: Int): Int = resultSet.getInt(i + 1)

      override def getLong(i: Int): Long = resultSet.getLong(i + 1)

      override def getObject(i: Int): Any = resultSet.getObject(i + 1)

      override def getShort(i: Int): Short = resultSet.getShort(i + 1)

      override def getTimestamp(i: Int): Timestamp = resultSet.getTimestamp(i + 1)
    }

    private var cursorMoved = false
    private var hasNextInvoked = false

    override def hasNext(): Boolean = {
      cursorMoved ||= (!hasNextInvoked && resultSet.next())
      hasNextInvoked = true
      cursorMoved
    }

    override def next(): Row = {
      if (!hasNext()) throw new NoSuchElementException("invoking next on empty Iterator[Row]")
      cursorMoved = false
      hasNextInvoked = false
      row
    }
  }
}

class JdbcRowSet(resultSet: ResultSet) extends RowSet {

  import JdbcRowSet._

  private class JdbcFieldDescriptor(i: Int) extends FieldDescriptor {
    override val name: String = resultSet.getMetaData.getColumnName(i)
    override val dataType: DataType =
      convertDataType(
        resultSet.getMetaData.getColumnType(i),
        resultSet.getMetaData.getPrecision(i),
        resultSet.getMetaData.getScale(i)
      )
    override val nullable: Boolean = resultSet.getMetaData.isNullable(i) == ResultSetMetaData.columnNullable
  }

  override val fields: Seq[FieldDescriptor] =
    for (i <- 1 to resultSet.getMetaData.getColumnCount) yield new JdbcFieldDescriptor(i)

  override def rows: Iterator[Row] = new JdbcRowIterator(resultSet)

  override def close(): Unit = resultSet.close()
}
