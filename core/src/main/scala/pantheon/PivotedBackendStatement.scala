package pantheon

import java.sql.{Date, Timestamp}

import pantheon.backend.{BackendPlan, BackendStatement}
import pantheon.planner.{OrderedColumn => _, _}
import pantheon.planner.literal.Literal
import pantheon.util.Logging.{ContextLogging, LoggingContext}
import pantheon.util.withResource

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import cats.syntax.either._

object PivotedBackendStatement {
  // used for grouping lazy values that will be initialized together
  protected case class Environment(
      columnFieldDescrs: Seq[FieldDescriptor],
      columnValues: Vector[Vector[Literal]],
      measureIndexForHeader: mutable.LinkedHashMap[Vector[Any], Int],
      valuesStmt: Statement
  ) {
    // number of 'buckets' with measures in the result of pivoted query.
    def outputColumnsCount: Int = measureIndexForHeader.size
  }

  protected case class FD(name: String, dataType: DataType, nullable: Boolean) extends FieldDescriptor

  protected class ArrayBackedRow(a: Array[Any], fields: Seq[FieldDescriptor]) extends Row {

    def get[T](i: Int)(implicit ct: ClassTag[T]) =
      if (a.size <= i || i < 0)
        throw new IndexOutOfBoundsException(s"Trying index $i of ArrayBackedRow of size ${a.size}")
      else if (a(i) == null) {
        if (fields(i).nullable) null.asInstanceOf[T]
        else throw new AssertionError(s"got null for non-nullable field ${fields(i)}")
      } else a(i).asInstanceOf[T]

    override def isNull(i: Int): Boolean = a(i) == null

    override def apply(i: Int): Any = get[Any](i)

    override def getBoolean(i: Int): Boolean = get[Boolean](i)

    override def getByte(i: Int): Byte = get[Byte](i)

    override def getDate(i: Int): Date = get[Date](i)

    override def getDecimal(i: Int): BigDecimal = get[java.math.BigDecimal](i)

    override def getDouble(i: Int): Double = get[Double](i)

    override def getFloat(i: Int): Float = get[Float](i)

    override def getInteger(i: Int): Int = get[Int](i)

    override def getLong(i: Int): Long = get[Long](i)

    override def getObject(i: Int): Any = get[Any](i)

    override def getShort(i: Int): Short = get[Short](i)

    override def getString(i: Int): String = get[String](i)

    override def getTimestamp(i: Int): Timestamp = get[Timestamp](i)
  }

  class PivotedStatementException(msg: String, cause: Throwable) extends Exception(msg, cause)
}

class PivotedBackendStatement(connection: Connection, val q: AggregateQuery, params: Map[String, Expression])(
    implicit protected val ctx: LoggingContext)
    extends BackendStatement
    with ContextLogging {

  import PivotedBackendStatement._
  logger.debug(s"Pivoted statement created with query: $q")
  import q.{columns => colDimensions, measures}
  import q.{rows => rowDimensions}
  private val rowsSz = rowDimensions.size
  private val rowInds = 0 until rowsSz
  private val colSz = colDimensions.size
  private val colInds = rowsSz until rowsSz + colSz
  private val mesuresSz = measures.size
  private val measureInds = colInds.last + 1 to colInds.last + mesuresSz
  private val valuesSize = measureInds.last + 1
  private val FetchColumnsStepName = "Query for column headers"
  private val FetchTopNRowsStepName = "Query for topN rows"
  private val FetchValuesStepName = "Query for values"

  private def createStmt(query: PantheonQuery, stepName: String): Statement =
    (for {
      _ <- Query.validatePantheon(query).toLeft(())
      s <- Either.fromTry(Try(connection.createStatement(query, params))).leftMap(_.toString)
    } yield s).valueOr(e => throw new AssertionError(s"failed to build $stepName: $e"))

  // Query for column headers
  private val columnsStatement = {
    createStmt(
      AggregateQuery(
        rows = colDimensions,
        // using measures here to link dimension tables (TODO: optimize this (whole pivoted query may be up to twice slower now))
        measures = measures,
        filter = q.filter,
        orderBy = colDimensions.map(OrderedColumn(_))
      ),
      FetchColumnsStepName
    )
  }

  private def buildValuesStmt(
      outputColumnsCount: Int
  ): Statement = {

    createStmt(
      AggregateQuery(
        rows = rowDimensions ::: colDimensions,
        measures = measures,
        filter = q.filter,
        offset = 0,
        // unable to precisely calculate offset to be pushed to db, fetching more data and dropping it after pivoting
        limit = q.limit.map(l => Math.multiplyExact(l + q.offset, outputColumnsCount)),
        orderBy = q.orderBy ++
          // rows are representing the compound key for the batch , need to order by all of them
          rowDimensions
            .filterNot(name => q.orderBy.exists(_.name == name))
            .map(pantheon.OrderedColumn(_)),
        aggregateFilter = q.aggregateFilter
      ),
      FetchValuesStepName
    )
  }

  private lazy val env: Environment = {

    val (columnValues: Vector[Vector[Literal]], columnFields: Seq[FieldDescriptor]) = catchStep(
      FetchColumnsStepName,
      withResource(columnsStatement.execute()) { columnsStmtRes =>
        // taking colSz not to include measures
        val columnFieldDescrs: Seq[FieldDescriptor] = columnsStmtRes.fields.take(colSz)
        val columnValues: Vector[Vector[Literal]] = columnsStmtRes.rows.map(getLiterals(columnFieldDescrs, _)).toVector
        columnValues -> columnFieldDescrs
      }
    )

    // output_column_header(represented by seq of dimension values) -> start_index of array where measures of a column will be written
    // saving order to reuse it while building FieldDescriptors of the result
    val measuresIndexForHeader: mutable.LinkedHashMap[Vector[Any], Int] = {
      val map = mutable.LinkedHashMap.empty[Vector[Any], Int]
      val valueIndices = Iterator.from(0).map(_ * mesuresSz + rowsSz)
      columnValues.foreach(v => map.put(v.map(_.value), valueIndices.next()))
      map
    }

    val valuesStmt: Statement = buildValuesStmt(measuresIndexForHeader.size)

    Environment(columnFields, columnValues, measuresIndexForHeader, valuesStmt)
  }

  override def logicalPlan: BackendPlan =
    mkPlan(
      catchStep(FetchValuesStepName, columnsStatement.backendLogicalPlan),
      catchStep(FetchTopNRowsStepName, None),
      catchStep(FetchValuesStepName, env.valuesStmt.backendLogicalPlan)
    )

  override def physicalPlan: BackendPlan =
    mkPlan(
      catchStep(FetchValuesStepName, columnsStatement.backendPhysicalPlan),
      catchStep(FetchTopNRowsStepName, None),
      catchStep(FetchValuesStepName, env.valuesStmt.backendPhysicalPlan)
    )

  override def execute(): RowSet = new RowSet {

    val valuesStmtRes = catchStep(FetchValuesStepName, env.valuesStmt.execute())

    override val fields: Vector[FieldDescriptor] = {

      val allFields = valuesStmtRes.fields

      // taking ranges not to rely on the fact that rows are coming first
      val rowFields = rowInds.headOption.map(allFields.drop(_).take(rowsSz)).getOrElse(Nil)
      val measureFields = measureInds.headOption.map(allFields.drop(_).take(mesuresSz)).getOrElse(Nil)

      val columnFields =
        for {
          (header, _) <- env.measureIndexForHeader
          mf <- measureFields
        } yield FD(header.mkString("->") + "->" + mf.name, mf.dataType, nullable = true)

      (rowFields ++ columnFields)(collection.breakOut)
    }

    override def rows: Iterator[Row] = {

      def pivotValues(values: RowSet): Iterator[Array[Any]] = {

        new Iterator[Array[Any]] {

          val fields = values.fields
          var _rows = values.rows

          override def hasNext: Boolean = _rows.hasNext

          override def next: Array[Any] = {
            if (!hasNext) throw new Exception("calling next on empty iterator")
            else {
              val nextDataRow = _rows.next()

              //need to save current values because RowSet is mutable and gets altered when onNext is called on rows iterator.
              val nextDataRowVals: Array[Any] = (0 until valuesSize).map(nextDataRow(_))(collection.breakOut)

              // need span here because takeWhile eats up last element
              // taking first chunk to be pivoted
              val (currentRowData, next) = _rows.span(r => rowInds.forall(i => r(i) == nextDataRowVals(i)))
              _rows = next

              //initializing Array that will hold an output row
              val arr = Array.ofDim[Any](rowsSz + env.outputColumnsCount * mesuresSz)

              //populating rows
              rowInds.foreach(v => arr.update(v, nextDataRowVals(v)))

              populateColumns(Iterator.single(new ArrayBackedRow(nextDataRowVals, fields)) ++ currentRowData, arr)

              arr
            }
          }

          // populating buffer array with columns
          private def populateColumns(i: Iterator[Row], buffer: Array[Any]): Unit = {
            i.foreach { nextRow =>
              val colHeader: Vector[Any] = colInds.map(nextRow(_))(collection.breakOut)
              val firstMeasureInColumnInd = env.measureIndexForHeader(colHeader)
              (0 until mesuresSz).foreach { j =>
                buffer.update(firstMeasureInColumnInd + j, nextRow(measureInds(j)))
              }
            }
          }
        }
      }

      val res = pivotValues(valuesStmtRes).map(new ArrayBackedRow(_, fields))
      // Applying query limit to the resulting iterator because there may be more rows than required in case when there are more columns then entries in unpivoted batches of values (sparse data).
      // Also applying offset here, impossible precisely to push to DB.
      q.limit.fold(res.drop(q.offset))(res.take(_))
    }

    override def close(): Unit = valuesStmtRes.close
  }

  override def cancel(): Try[Unit] = {
    val c1 = columnsStatement.cancel()
    val c2 = Success(())
    val c3 = env.valuesStmt.cancel()

    val errOpts: List[Option[Throwable]] = List(c1, c2, c3).map(Either.fromTry(_).left.toOption)

    if (errOpts.forall(_.isEmpty)) Success(())
    else {
      val labeledErrors: List[(Throwable, String)] =
        errOpts
          .zip(Seq(FetchColumnsStepName, FetchTopNRowsStepName, FetchValuesStepName))
          .flatMap { case (err, lbl) => err.map(_ -> lbl) }

      Failure(
        new Exception(labeledErrors.map { case (e, lbl) => s"Error cancelling '$lbl': $e" }.mkString(","),
                      labeledErrors.head._1)
      )
    }

  }

  private def mkPlan(s1: BackendPlan, s2: Option[BackendPlan], s3: BackendPlan) = new BackendPlan {

    def formatPlanEntry(name: String, plan: BackendPlan) =
      s"""
           |$name:
           |${plan.canonical}""".stripMargin

    override val canonical: String =
      formatPlanEntry(FetchColumnsStepName, s1) ++
        s2.map(formatPlanEntry(FetchTopNRowsStepName, _)).getOrElse("") ++
        formatPlanEntry(FetchValuesStepName, s3)

    override def toString: String = canonical

  }

  private def catchStep[T](stepLbl: String, block: => T): T =
    try {
      block
    } catch {
      // exposing the real problem
      case t: PivotedStatementException => throw t
      case NonFatal(t)                  => throw new PivotedStatementException(s"step '$stepLbl' failed", t)
    }

  private def getLiterals(fields: Seq[FieldDescriptor], row: Row): Vector[Literal] =
    fields.zipWithIndex.map {
      case (fd, i) =>
        // TODO: code duplication! (this part is taken from RowSetUtil.toStringList)
        def withNullCheck[T](v: Int => T)(f: T => Literal): Literal =
          if (row.isNull(i))
            if (fd.nullable) literal.Null
            else throw new AssertionError(s"got null for non-nullable field $fd")
          else f(v(i))

        fields(i).dataType match {
          case BooleanType =>
            withNullCheck(row.getBoolean)(literal.Boolean)
          case ByteType =>
            withNullCheck(row.getByte)(b => literal.Integer(b.toInt))
          case DateType =>
            withNullCheck(row.getDate)(literal.Date)
          case _: DecimalType =>
            withNullCheck(row.getDecimal)(literal.Decimal)
          case DoubleType =>
            withNullCheck(row.getDouble)(literal.Double)
          case FloatType =>
            withNullCheck(row.getFloat)(f => literal.Double(f.toDouble))
          case IntegerType =>
            withNullCheck(row.getInteger)(literal.Integer)
          case LongType =>
            withNullCheck(row.getLong)(literal.Long)
          case NullType => literal.Null
          case ObjectType =>
            withNullCheck(row.getObject)(s => literal.String(s.toString))
          case ShortType =>
            withNullCheck(row.getShort)(s => literal.Integer(s.toInt))
          case StringType =>
            withNullCheck(row.getString)(literal.String)
          case TimestampType =>
            withNullCheck(row.getTimestamp)(literal.Timestamp)
        }
    }(collection.breakOut)

}
