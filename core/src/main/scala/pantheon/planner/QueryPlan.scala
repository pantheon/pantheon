package pantheon.planner

import java.sql

import pantheon.{OrderedColumn => _, _}
import pantheon.planner.literal.{Literal, NumericLiteral}
import pantheon.schema._
import pantheon.RecordQuery
import cats.syntax.either._

object QueryPlan {

  def fromQuery(q: BasicPantheonQuery, defaultFilter: Option[Predicate]): QueryPlan = {

    val (fields, isEntity, aggFilter) = q match {
      case q: AggregateQuery => (q.rows ::: q.measures, false, q.aggregateFilter)
      case q: RecordQuery    => (q.rows, true, None)
    }

    val outputFields = fields.map(UnresolvedField)

    def mapOrderBy(orderBy: List[pantheon.OrderedColumn]): List[OrderedColumn] =
      orderBy.map(oc => OrderedColumn(UnresolvedField(oc.name), oc.order))

    def aggregateFilter(child: QueryPlan): QueryPlan =
      aggFilter.fold[QueryPlan](child)(Filter(_, child))

    def filter(child: QueryPlan): QueryPlan =
      (q.filter ++ defaultFilter).reduceOption(And).fold[QueryPlan](child)(Filter(_, child))

    def aggregate(child: QueryPlan): QueryPlan =
      if (isEntity) child
      else SchemaAggregate(child = child)

    Limit(q.offset,
          q.limit,
          Sort(mapOrderBy(q.orderBy),
               SchemaProject(outputFields, aggregateFilter(aggregate(filter(UnresolvedSchema()))))))
  }

  def pprint(v: Aggregation): String = s"Agg(${v.aggregator}, ${v.tableColumn.name})"
  def pprint(o: OrderedColumn): String = s"OrderedCol(${pprint(o.ref)}, order=${o.order})"

  def pprint(v: QueryPlan, indent: Int = 0): String = {
    val tab = " " * 2 * indent
    def listPrint(s: Iterable[String]) = s"[${s.mkString(", ")}]"
    def printFlds(flds: Seq[FieldRef]) = listPrint(flds.map(pprint(_)))

    val content = v match {
      case f: UnresolvedSchema => "UnresolvedSchema "

      case ResolvedSchema(schema, usedAttributes, outputDimensionAttributes, outputMeasures) =>
        s"ResolvedSchema[schema=${schema.name}, usedDimensionAttributes=${listPrint(usedAttributes.map(_.name))}, outputDimensionAttributes=${listPrint(
          outputDimensionAttributes.map(_.name))}, outputMeasures=${listPrint(outputMeasures.map(_.name))}"

      case SuitableTables(schema, usedAttributes, outputDimensionAttributes, outputMeasures, tables) =>
        s"SuitableTables[schema=${schema.name}, usedDimensionAttributes=${listPrint(usedAttributes.map(_.name))}, outputDimensionAttributes=${listPrint(outputDimensionAttributes.map(
          _.name))}, outputMeasures=${listPrint(outputMeasures.map(_.name))}, tables=${listPrint(tables.map(_._1.name))}"

      case agg: Aggregate =>
        s"Aggregate[group=${listPrint(agg.groupColumns.map(pprint(_)))}, aggregations=${listPrint(
          agg.aggregations.map(pprint))}]\n${pprint(agg.child, indent + 1)}"

      case f: Filter => s"Filter[${f.condition}]\n${pprint(f.child, indent + 1)}"

      case l: Limit =>
        s"Limit${listPrint(Seq("offset=" + l.offset).++(l.limit.map("limit=" + _)))}\n${pprint(l.child, indent + 1)}"

      case j: Join =>
        s"Join[${pprint(j.leftColumn)} = ${pprint(j.rightColumn)}, type=${j.joinType}]\n${pprint(
          j.leftChild,
          indent + 1)}\n${pprint(j.rightChild, indent + 1)}"

      //TODO: figure out how to print SuperJoin properly
      case SuperJoin(plans, usedAttributes, outputAttributes, outputMeasures) =>
        s"SuperJoin[usedDimensionAttributes=${listPrint(usedAttributes.map(_.name))}, outputDimensionAttributes=${listPrint(outputAttributes
          .map(_.name))}, measures=${outputMeasures.map(s => listPrint(s.toSeq.map(pprint(_))))}]\n${plans.map(pprint(_, indent + 1)).mkString("\n")}"

      case j: JoinUsing =>
        s"JoinUsing[columns=${listPrint(j.columns)}]\n${Seq(j.leftChild, j.rightChild).map(pprint(_, indent + 1)).mkString("\n")}"

      case v: View => s"View[table=${v.table.name}, expressions=${listPrint(v.expressions.map(_.name))}]"

      case p: SchemaProject => s"SchemaProject[fields=${printFlds(p.fields)}]\n${pprint(p.child, indent + 1)}"

      case SchemaAggregate(outputDimensionAttributes, outputMeasures, child) =>
        s"SchemaAggregate[outputDimensionAttributes=${listPrint(outputDimensionAttributes.map(_.name))}, outputMeasures=${listPrint(
          outputMeasures.map(_.name))}]\n${pprint(child, indent + 1)}"

      case p: Sort => s"Sort[fields=${listPrint(p.columns.map(pprint))}]\n${pprint(p.child, indent + 1)}"

      case c: ColumnRef => s"c(${c.name})"

      case da: ResolvedDimensionAttribute => s"d(${da.name})"

      case m: ResolvedMeasure => s"m(${m.name})"

      case ResolvedCalculatedMeasure(name, _, _) => s"m($name)"

      case ne: NamedExpression => s"c(${ne.name})"

      case t: Project =>
        s"Project[cols=${listPrint(t.columns.map(pprint(_)))}\n${pprint(t.child, indent + 1)}"

      case t: TableScan => s"TableScan[${t.table.name}]"

      case n => n.toString
    }
    tab + content
  }
}

abstract class QueryPlan extends TreeNode[QueryPlan] {
  // TODO input parameters, output parameters
}

trait Expression extends QueryPlan

trait Predicate extends Expression

trait NumericExpressionAST

trait NumericExpression extends Expression

object NumericExpression {
  def collectMeasures(n: NumericExpression): Set[Either[AggMeasure, FilteredMeasure]] =
    n match {
      case b: NumericBinaryExpr => collectMeasures(b.left) ++ collectMeasures(b.right)
      case m: AggMeasure        => Set(Left(m))
      case m: FilteredMeasure   => Set(Right(m))
      case _: Literal           => Set.empty
    }
}

//
// Basic expressions
//

object literal {
  sealed trait Literal extends Expression {
    val value: Any
  }

  trait NumericLiteral extends NumericExpression with NumericExpressionAST

  case class Timestamp(value: sql.Timestamp) extends Literal
  case class Date(value: sql.Date) extends Literal
  case class Boolean(value: scala.Boolean) extends Predicate with Literal
  case class Integer(value: Int) extends Literal with NumericLiteral
  case class Long(value: scala.Long) extends Literal with NumericLiteral
  case class Double(value: scala.Double) extends Literal with NumericLiteral
  case class Decimal(value: BigDecimal) extends Literal with NumericLiteral
  case class String(value: Predef.String) extends Literal
  case object Null extends Literal {
    val value = null
  }
}

case class BindVariable(name: String) extends Expression

sealed trait FieldRef extends Expression { def name: String }

//We assume that any field may be a number (having no types in the schema)
case class UnresolvedField(name: String) extends FieldRef with NumericExpressionAST

private[pantheon] case class ResolvedDimensionAttribute(name: String, dimensionAttribute: Attribute) extends FieldRef

private[pantheon] case class ResolvedMeasure(name: String,
                                             measure: Either[AggMeasure, FilteredMeasure],
                                             requiredDimAttrs: Set[ResolvedDimensionAttribute])
    extends FieldRef {
  def baseName = measure.fold(_.name, _.base.name)
}

private[pantheon] case class ResolvedCalculatedMeasure(
    name: String,
    expression: NumericExpression,
    requiredMeasures: Set[ResolvedMeasure]
) extends FieldRef

//
// Predicates
//
/*
TODO: Proposal: make names Shorter (Eq, Neq, Gt, GtEq, LtEq), use l and r for left and right
Easier to write and read
 */
case class EqualTo(left: Expression, right: Expression) extends Predicate
case class IsNull(e: Expression) extends Predicate
case class IsNotNull(e: Expression) extends Predicate
case class NotEqual(left: Expression, right: Expression) extends Predicate
case class GreaterThan(left: Expression, right: Expression) extends Predicate
case class LessThan(left: Expression, right: Expression) extends Predicate
case class GreaterThanOrEqual(left: Expression, right: Expression) extends Predicate
case class LessThanOrEqual(left: Expression, right: Expression) extends Predicate
case class In(left: Expression, right: Seq[Expression]) extends Predicate
case class NotIn(left: Expression, right: Seq[Expression]) extends Predicate
case class Like(left: Expression, right: Seq[Expression]) extends Predicate
case class And(left: Predicate, right: Predicate) extends Predicate
case class Or(left: Predicate, right: Predicate) extends Predicate

// parsed expr
case class PlusAST(left: NumericExpressionAST, right: NumericExpressionAST) extends NumericExpressionAST
case class MinusAST(left: NumericExpressionAST, right: NumericExpressionAST) extends NumericExpressionAST
case class DivideAST(left: NumericExpressionAST, right: NumericExpressionAST) extends NumericExpressionAST
case class MultiplyAST(left: NumericExpressionAST, right: NumericExpressionAST) extends NumericExpressionAST

// compiled expr
sealed trait NumericBinaryExpr extends NumericExpression {
  def left: NumericExpression
  def right: NumericExpression
  val _copy: (NumericExpression, NumericExpression) => NumericBinaryExpr
}
case class Plus(left: NumericExpression, right: NumericExpression) extends NumericBinaryExpr {
  override val _copy = copy _
}
case class Minus(left: NumericExpression, right: NumericExpression) extends NumericBinaryExpr {
  override val _copy = copy _
}
case class Divide(left: NumericExpression, right: NumericExpression) extends NumericBinaryExpr {
  override val _copy = copy _
}
case class Multiply(left: NumericExpression, right: NumericExpression) extends NumericBinaryExpr {
  override val _copy = copy _
}

case class Cast(castType: String, expr: Expression) extends Expression

//
// Schema nodes
//

case class SchemaProject(fields: List[FieldRef], child: QueryPlan) extends QueryPlan

case class SchemaAggregate(outputDimensionAttributes: List[ResolvedDimensionAttribute] = List(),
                           outputMeasures: List[ResolvedMeasure] = List(),
                           child: QueryPlan)
    extends QueryPlan

case class UnresolvedSchema() extends QueryPlan

case class ResolvedSchema(schema: Schema,
                          usedDimensionAttributes: List[ResolvedDimensionAttribute] = List(),
                          outputDimensionAttributes: List[ResolvedDimensionAttribute] = List(),
                          outputMeasures: List[ResolvedMeasure] = List())
    extends QueryPlan

case class SuitableTables(schema: Schema,
                          usedDimensionAttributes: List[ResolvedDimensionAttribute],
                          outputDimensionAttributes: List[ResolvedDimensionAttribute],
                          outputMeasures: List[ResolvedMeasure] = List(),
                          tables: Seq[(Table, Set[ResolvedMeasure])] = Seq())
    extends QueryPlan

case class SuperJoin(plans: List[QueryPlan],
                     usedDimensionAttributes: List[ResolvedDimensionAttribute],
                     outputDimensionAttributes: List[ResolvedDimensionAttribute],
                     outputMeasures: List[Set[ResolvedMeasure]])
    extends QueryPlan

case class TableScan(schema: Schema,
                     table: Table,
                     outputDimensionAttributes: Set[ResolvedDimensionAttribute] = Set(),
                     outputMeasures: Set[ResolvedMeasure] = Set())
    extends QueryPlan

//
// Relational nodes
//

case class ColumnRef(name: String, table: Option[String] = None) extends Expression with NumericExpression

case class NamedExpression(name: String, expression: Expression) extends QueryPlan

case class Aggregation(tableColumn: ColumnRef, alias: String, aggregator: MeasureAggregate, filter: Option[Predicate])

case class Project(columns: List[NamedExpression], child: QueryPlan) extends QueryPlan

case class Aggregate(groupColumns: List[ColumnRef], aggregations: List[Aggregation], child: QueryPlan) extends QueryPlan

case class Filter(condition: Predicate, child: QueryPlan) extends QueryPlan

case class Limit(offset: Int, limit: Option[Int], child: QueryPlan) extends QueryPlan

case class OrderedColumn(ref: FieldRef, order: SortOrder.Value = SortOrder.Asc)

case class Sort(columns: List[OrderedColumn], child: QueryPlan) extends QueryPlan

case class Join(leftChild: QueryPlan,
                rightChild: QueryPlan,
                leftColumn: ColumnRef,
                rightColumn: ColumnRef,
                joinType: JoinType.Value)
    extends QueryPlan

case class JoinUsing(leftChild: QueryPlan, rightChild: QueryPlan, columns: List[String]) extends QueryPlan

case class ViewExpression(name: String, expression: String)
case class View(table: Table, expressions: List[ViewExpression] = Nil) extends QueryPlan
