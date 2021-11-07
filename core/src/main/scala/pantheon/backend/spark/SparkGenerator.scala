package pantheon.backend.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import pantheon.SortOrder
import pantheon.planner._
import pantheon.planner.literal.{Literal, NumericLiteral}
import pantheon.schema.{CalculatedMeasure, Measure}

object SparkGenerator {
  val RowNumberColumnName = "__rownumber"
}

class SparkGenerator(tables: Map[String, DataFrame]) {

  import SparkGenerator._

  private def table(name: String): DataFrame = {
    assert(tables.contains(name), s"Table not found: $name")
    tables(name)
  }

  private def columnToExpr(tableColumn: ColumnRef): Column =
    tableColumn.table.map(t => table(t)(q(tableColumn.name))).getOrElse(col(q(tableColumn.name)))

  private def columnToAliasedExpr(namedExpression: NamedExpression): Column =
    translateExpression(namedExpression.expression, columnToExpr).as(namedExpression.name)

  private def q(name: String) = s"`$name`"

  private def translateLiteral(l: Literal): Column = l match {
    case literal.String(v)    => typedLit(v)
    case literal.Boolean(v)   => typedLit(v)
    case literal.Integer(v)   => typedLit(v)
    case literal.Long(v)      => typedLit(v)
    case literal.Double(v)    => typedLit(v)
    case literal.Decimal(v)   => typedLit(v)
    case literal.Timestamp(v) => typedLit(v)
    case literal.Date(v)      => typedLit(v)
    case literal.Null         => typedLit(null)
  }

  private def translateExpression(expr: Expression, translateTableCol: ColumnRef => Column): Column = {

    def _translateExpression(expr: Expression): Column = expr match {
      case l: Literal   => translateLiteral(l)
      case c: ColumnRef => translateTableCol(c)
      case EqualTo(left, right) =>
        _translateExpression(left) === _translateExpression(right)
      case IsNull(e)    => _translateExpression(e).isNull
      case IsNotNull(e) => _translateExpression(e).isNotNull
      case NotEqual(left, right) =>
        _translateExpression(left) =!= _translateExpression(right)
      case GreaterThan(left, right) =>
        _translateExpression(left) > _translateExpression(right)
      case LessThan(left, right) =>
        _translateExpression(left) < _translateExpression(right)
      case GreaterThanOrEqual(left, right) =>
        _translateExpression(left) >= _translateExpression(right)
      case LessThanOrEqual(left, right) =>
        _translateExpression(left) <= _translateExpression(right)
      case In(left, right) =>
        val l = _translateExpression(left)
        right.map(v => l === _translateExpression(v)).reduceLeft(_ || _)
      case NotIn(left, right) =>
        val l = _translateExpression(left)
        right.map(v => l =!= _translateExpression(v)).reduceLeft(_ && _)
      case Like(left, right) =>
        val l = _translateExpression(left)
        right
          .map { r =>
            _translateExpression(r).expr match {
              case org.apache.spark.sql.catalyst.expressions.Literal(value, dataType) if dataType == StringType =>
                l.like(value.toString)
              case _ => throw new Exception("Only string literals are supported in Like")
            }
          }
          .reduceLeft(_ || _)
      case And(left, right) =>
        _translateExpression(left) && _translateExpression(right)
      case Or(left, right) =>
        _translateExpression(left) || _translateExpression(right)
      case Plus(l, r)        => _translateExpression(l) + _translateExpression(r)
      case Minus(l, r)       => _translateExpression(l) - _translateExpression(r)
      case Divide(l, r)      => _translateExpression(l) / _translateExpression(r)
      case Multiply(l, r)    => _translateExpression(l) * _translateExpression(r)
      case Cast(castType, e) =>
        // TODO implement cast
        _translateExpression(e)

      case node =>
        throw new AssertionError(s"Unsupported node in expression: $node")
    }

    _translateExpression(expr)
  }

  def generate(plan: QueryPlan): DataFrame = {
    plan match {
      case TableScan(_, t, _, _) => table(t.name)

      case View(t, _) => table(t.name)

      case Join(left, right, leftCol, rightCol, joinType) =>
        val l = generate(left)
        val r = generate(right)
        l.join(r, l(q(leftCol.name)) === r(q(rightCol.name)), joinType.toString.toLowerCase)

      case JoinUsing(left, right, columns) =>
        val l = generate(left)
        val r = generate(right)
        if (columns.isEmpty) l.crossJoin(r)
        else l.join(r, columns, "full")

      case Filter(condition, child) =>
        generate(child).filter(translateExpression(condition, columnToExpr))

      case Project(columns, child) =>
        val c = generate(child)
        c.select(columns.map(columnToAliasedExpr): _*)

      case Limit(offset, limit, child) =>
        val c = generate(child)
        if (offset > 0) throw new Exception("Limit is not implemented for Spark backend")
        limit.map(l => c.limit(l)).getOrElse(c)

      case Sort(columns, child) =>
        val c = generate(child)
        val exprs = columns.map { col =>
          val sc = c(q(col.ref.name))
          col.order match {
            case SortOrder.Asc  => sc.asc_nulls_last
            case SortOrder.Desc => sc.desc_nulls_last
          }
        }
        c.orderBy(exprs: _*)

      case Aggregate(columns, aggregations, child) =>
        import pantheon.schema.MeasureAggregate._

        val c = generate(child)

        val aggs = aggregations
          .map { a =>
            val alias = a.alias
            val field = c(q(a.tableColumn.name))

            val filteredField = a.filter
              .map(filter =>
                when(
                  translateExpression(
                    // operating on the level of aliases here because aggregations comes above Tabular projects which map column names to aliases
                    filter,
                    tc => c(q(tc.name))
                  ),
                  field
              ))
              .getOrElse(field)

            a.aggregator match {
              case Sum                 => sum(filteredField).as(alias)
              case Count               => count(filteredField).as(alias)
              case DistinctCount       => countDistinct(filteredField).as(alias)
              case ApproxDistinctCount => approx_count_distinct(filteredField).as(alias)
              case Avg                 => avg(filteredField).as(alias)
              case Min                 => min(filteredField).as(alias)
              case Max                 => max(filteredField).as(alias)
            }
          }

        val group = c.groupBy(columns.map(col => c(q(col.name))): _*)
        aggs.headOption.map(agg => group.agg(agg, aggs.tail: _*)).getOrElse(group.agg(Map.empty[String, String]))
    }
  }
}
