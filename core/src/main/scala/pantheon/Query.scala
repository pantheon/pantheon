package pantheon

import pantheon.planner.{Predicate, UnresolvedField}
import pantheon.util.BoolOps
import cats.syntax.list._
import cats.syntax.either._

object SortOrder extends Enumeration {
  val Asc, Desc = Value
}

case class TopN(orderBy: List[OrderedColumn], dimensions: List[String], n: Int)

case class OrderedColumn(name: String, order: SortOrder.Value = SortOrder.Asc)

sealed trait Query

sealed trait PantheonQuery extends Query {
  val filter: Option[Predicate]
  val offset: Int
  val limit: Option[Int]
  val orderBy: List[OrderedColumn]
  def rows: List[String]
}

// Marker trait for queries that are executed by pantheon engine and in terms of which higher level queries are built (e.g., PivotedQuery)
sealed trait BasicPantheonQuery extends PantheonQuery

object Query {

  private def validateOrderRefsAgainst(lbl: String,
                                       orderBy: List[OrderedColumn],
                                       fldLists: Seq[String]*): Option[String] = {
    val orderRefs = orderBy.map(_.name)
    val refsNotInRows = orderRefs.filterNot(r => fldLists.exists(_.contains(r)))
    refsNotInRows.nonEmpty.option(
      s"the following orderBy references ${refsNotInRows.mkString("[", ",", "]")} were not found in '$lbl'"
    )
  }

  def validateAggregate(q: AggregateQuery): Option[String] = {
    if (q.pivoted) {
      q.columns.isEmpty.option("columns must not be empty in Pivoted query") orElse
        q.measures.isEmpty.option("measures must not be empty in Pivoted query") orElse
        validateOrderRefsAgainst("rows", q.orderBy, q.rows)
    } else {
      (q.measures.isEmpty && q.rows.isEmpty)
        .option("both measures and rows are not defined in Aggregate query") orElse
        validateOrderRefsAgainst("rows and measures", q.orderBy, q.rows, q.measures)
    }
  }

  def validateEntity(q: RecordQuery): Option[String] =
    q.rows.isEmpty.option("Rows must not be empty in Record query") orElse
      validateOrderRefsAgainst("rows", q.orderBy, q.rows)

  def validatePantheon(pq: PantheonQuery): Option[String] =
    pq match {
      case q: AggregateQuery => validateAggregate(q)
      case q: RecordQuery    => validateEntity(q)
    }

  def validate(pq: Query): Option[String] =
    pq match {
      case q: SqlQuery      => None
      case q: PantheonQuery => validatePantheon(q)
    }
}

case class AggregateQuery(
    columns: List[String] = Nil,
    measures: List[String] = Nil,
    rows: List[String] = Nil,
    filter: Option[Predicate] = None,
    offset: Int = 0,
    limit: Option[Int] = None,
    orderBy: List[OrderedColumn] = Nil,
    aggregateFilter: Option[Predicate] = None
) extends BasicPantheonQuery {
  def pivoted: Boolean = columns.nonEmpty
}

case class RecordQuery(
    rows: List[String],
    filter: Option[Predicate] = None,
    offset: Int = 0,
    limit: Option[Int] = None,
    orderBy: List[OrderedColumn] = Nil
) extends BasicPantheonQuery

case class SqlQuery(sql: String) extends Query {
  def value = this
}
