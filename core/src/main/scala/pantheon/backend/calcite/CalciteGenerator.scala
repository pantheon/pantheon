package pantheon.backend.calcite

import java.util.Calendar

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.Convention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexFieldCollation, RexNode, RexWindowBound}
import org.apache.calcite.sql.{SqlKind, SqlWindow}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{DateString, TimestampString}
import pantheon.util.Tap
import pantheon.{OrderedColumn, SortOrder}
import pantheon.planner._
import pantheon.planner.literal.{Literal, NumericLiteral}
import pantheon.schema.{JoinType, Measure, Table}
import pantheon.util.Logging.{ContextLogging, LoggingContext}

import scala.collection.JavaConverters._

class CalciteGeneratorException(message: String, cause: Throwable = null) extends Exception(message, cause)

class CalciteGenerator(calciteConnection: CalciteConnection) extends ContextLogging {

  private def translateLiteral(lit: Literal, builder: RelBuilder): RexNode = {

    lit match {
      case literal.String(v)  => builder.literal(v)
      case literal.Boolean(v) => builder.literal(v)
      case literal.Integer(v) => builder.literal(v)
      case literal.Long(v)    => builder.literal(v)
      case literal.Double(v)  => builder.literal(v)
      case literal.Decimal(v) => builder.literal(v)
      case literal.Timestamp(ts) =>
        builder.getRexBuilder.makeTimestampLiteral(
          TimestampString.fromCalendarFields(Calendar.getInstance().tap(_.setTime(ts))),
          0
        )
      case literal.Date(date) =>
        builder.getRexBuilder.makeDateLiteral(
          DateString.fromCalendarFields(Calendar.getInstance().tap(_.setTime(date)))
        )
      case literal.Null => builder.literal(null)
    }
  }

  private def columnToField(tableName: Option[String],
                            colName: String,
                            builder: RelBuilder,
                            stackDepth: Int = 1): RexNode =
    try {
      tableName.map(t => builder.field(stackDepth, t, colName)).getOrElse(builder.field(colName))
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Cannot find column '$colName'" +
                                             tableName.map(n => s" in table '$n'").getOrElse(""),
                                           e)
    }

  private def translateNamedExpr(ne: NamedExpression, builder: RelBuilder): RexNode =
    builder.alias(translateExpression(ne.expression, builder), ne.name)

  private def translateExpression(expr: Expression, builder: RelBuilder): RexNode = {
    def _translateExpression(expr: Expression): RexNode = expr match {
      case l: Literal   => translateLiteral(l, builder)
      case c: ColumnRef => columnToField(c.table, c.name, builder)
      case EqualTo(left, right) =>
        builder.equals(_translateExpression(left), _translateExpression(right))
      case IsNull(e) =>
        builder.isNull(_translateExpression(e))
      case IsNotNull(e) =>
        builder.isNotNull(_translateExpression(e))
      case NotEqual(left, right) =>
        builder.call(SqlStdOperatorTable.NOT_EQUALS, _translateExpression(left), _translateExpression(right))
      case GreaterThan(left, right) =>
        builder.call(SqlStdOperatorTable.GREATER_THAN, _translateExpression(left), _translateExpression(right))
      case LessThan(left, right) =>
        builder.call(SqlStdOperatorTable.LESS_THAN, _translateExpression(left), _translateExpression(right))
      case GreaterThanOrEqual(left, right) =>
        builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, _translateExpression(left), _translateExpression(right))
      case LessThanOrEqual(left, right) =>
        builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, _translateExpression(left), _translateExpression(right))
      case In(left, right) =>
        val eqs = right.map(v => builder.equals(_translateExpression(left), _translateExpression(v)))
        builder.or(eqs.asJava)
      case NotIn(left, right) =>
        val noeqs = right.map(
          v =>
            builder
              .call(SqlStdOperatorTable.NOT_EQUALS, _translateExpression(left), _translateExpression(v)))
        builder.and(noeqs.asJava)
      case Like(left, right) =>
        val likes =
          right.map(v => builder.call(SqlStdOperatorTable.LIKE, _translateExpression(left), _translateExpression(v)))
        builder.or(likes.asJava)
      case And(left, right) =>
        builder.and(_translateExpression(left), _translateExpression(right))

      case Or(left, right) =>
        builder.or(_translateExpression(left), _translateExpression(right))

      case Plus(l, r) =>
        builder.call(SqlStdOperatorTable.PLUS, _translateExpression(l), _translateExpression(r))
      case Minus(l, r) =>
        builder.call(SqlStdOperatorTable.MINUS, _translateExpression(l), _translateExpression(r))
      case Divide(l, r) =>
        val right = _translateExpression(r)
        val left = _translateExpression(l)
        builder
          .call(
            SqlStdOperatorTable.CASE,
            builder.or(builder.isNull(left), builder.isNull(right), builder.equals(right, builder.literal(0))),
            builder.literal(null),
            // casting to double here as a workaround for what looks like a Calcite problem. It tries to interpret result of division as Long and fails(but only when used with Clickhouse).
            // checking for nulls because ClickHouse gives an error when trying to cast nulls to numbers(operation with null returns null)
            builder.cast(builder.call(SqlStdOperatorTable.DIVIDE, left, right), SqlTypeName.DOUBLE)
          )

      case Multiply(l, r) =>
        builder.call(SqlStdOperatorTable.MULTIPLY, _translateExpression(l), _translateExpression(r))

      case Cast(castType, e) =>
        builder.cast(_translateExpression(e), getSqlType(castType))

      case node =>
        throw new AssertionError(s"Unsupported node in expression: $node")
    }

    _translateExpression(expr)
  }

  /**
    * See available type names in [[org.apache.calcite.sql.type.SqlTypeName]]
    *
    * @param name the name of the SQL type as it is defined in [[SqlTypeName]] in any case (possibly lower case to
    *             satisfy style of schema definition)
    * @return
    */
  private def getSqlType(name: String): SqlTypeName = {
    SqlTypeName.get(name.toUpperCase)
  }

  private def getJoinRelType(joinType: JoinType.Value): JoinRelType = {
    joinType match {
      case JoinType.Inner =>
        JoinRelType.INNER
      case JoinType.Left =>
        JoinRelType.LEFT
      case JoinType.Right =>
        JoinRelType.RIGHT
      case JoinType.Full =>
        JoinRelType.FULL
    }
  }

  /**
    * Generates Calcite plan from the Pantheon plan
    *
    * @param plan Pantheon query plan
    * @return Calcite query plan
    */
  def generate(plan: QueryPlan)(implicit ctx: LoggingContext): CalcitePlan = {
    val builder = calciteConnection.relBuilder

    def replaceTop(newTop: RelNode): RelBuilder = {
      val method = classOf[RelBuilder].getDeclaredMethod("replaceTop", classOf[RelNode])
      method.setAccessible(true)
      method.invoke(builder, newTop)
      builder
    }

    def translateRef(ref: FieldRef): RexNode = ref match {
      case ResolvedDimensionAttribute(name, dimAttr) =>
        val col = builder.field(name)
        dimAttr.castType.map(t => builder.cast(col, getSqlType(t))).getOrElse(col)

      case ResolvedMeasure(_, m, _) =>
        builder.field(m.merge.name)

      case ResolvedCalculatedMeasure(name, calc, _) => builder.alias(translateExpression(calc, builder), name)

      case x: UnresolvedField =>
        throw new AssertionError(
          s"Unexpected situation: Unresolved Dimension/Measure encountered in CalciteGenerator $x"
        )

    }

    plan foreachUp {

      case TableScan(_, table, _, _) =>
        //Discuss: How do we know that physicalTableOrSql will be 'Left' here??
        builder.scan(table.dataSource.name, table.definition.ensuring(_.isLeft).left.get.value).as(table.name)

      case View(table, _) =>
        builder.push(calciteConnection.tableToRel(table)).as(table.name)

      case Join(left, right, leftCol, rightCol, joinType) =>
        builder.join(getJoinRelType(joinType),
                     builder.equals(
                       columnToField(leftCol.table, leftCol.name, builder, 2),
                       columnToField(rightCol.table, rightCol.name, builder, 2)
                     ))

      case JoinUsing(_, _, columns) =>
        def getTableFields(inputOrdinal: Int) = {
          val rowType = builder.peek(2, inputOrdinal).getRowType
          rowType.getFieldNames.asScala.filterNot(columns.contains)
        }

        val commonFields = columns.map(c => (builder.field(2, 0, c), builder.field(2, 1, c)))
        val t1Fields = getTableFields(0)
        val t2Fields = getTableFields(1)

        val conditions = commonFields.map {
          case (f1, f2) =>
            builder.call(SqlStdOperatorTable.EQUALS, f1, f2)
        }
        builder.join(JoinRelType.FULL, conditions.asJava)
        builder.push(builder.build()) // this line fixes bug in building join (https://issues.apache.org/jira/browse/CALCITE-2626)

        val projFields = columns.zip(commonFields).map {
          case (name, (f1, f2)) =>
            val left = builder.field(f1.getIndex)
            val right = builder.field(f2.getIndex)
            builder.alias(builder.call(SqlStdOperatorTable.CASE, builder.isNotNull(left), left, right), name)
        }

        builder.project((projFields ++ t1Fields.map(builder.field) ++ t2Fields.map(builder.field)).asJava)

      case Filter(condition, _) =>
        builder.filter(
          translateExpression(condition, builder)
        )

      case Project(columns, _) =>
        builder.project(columns.map(c => translateNamedExpr(c, builder)).asJava, ImmutableList.of[String](), true)

      case Limit(offset, limit, _) => builder.limit(offset, limit.getOrElse(-1))

      case Sort(vals, _) =>
        builder.sort(
          vals
            .map { v =>
              val rex = translateRef(v.ref)
              if (v.order == SortOrder.Desc) builder.nullsLast(builder.desc(rex))
              else builder.nullsLast(rex)
            }: _*
        )

      case Aggregate(groupColumns, aggregations, _) =>
        import pantheon.schema.MeasureAggregate._

        val group = groupColumns.map(c => builder.field(c.name))
        val agg = aggregations.map { a =>
          val alias = a.alias
          def field = builder.field(a.tableColumn.name)

          val node = a.filter
            .map(
              filter =>
                //example is taken from SqlToRelConverter.java  1270
                builder
                  .call(
                    SqlStdOperatorTable.CASE,
                    translateExpression(filter, builder),
                    field,
                    builder.literal(null)
                )
            )
            .getOrElse(field)

          a.aggregator match {

            case Sum                 => builder.sum(false, alias, node)
            case Avg                 => builder.avg(false, alias, node)
            case Count               => builder.count(false, alias, node)
            case DistinctCount       => builder.count(true, alias, node)
            case ApproxDistinctCount =>
              // SqlAggFunction aggFunction, boolean distinct,
              //      boolean approximate, RexNode filter, ImmutableList<RexNode> orderKeys,
              //      String alias, ImmutableList<RexNode> operands
              builder
                .aggregateCall(SqlStdOperatorTable.COUNT, node)
                .distinct()
                .approximate(true)
                .as(alias)
            case Min => builder.min(alias, node)
            case Max => builder.max(alias, node)
          }
        }

        builder.aggregate(builder.groupKey(group.asJava), agg.asJava)

      case _ =>
    }
    val calcitePlan = new CalcitePlan(builder.build())
    logger.debug(s"Calcite plan: $calcitePlan")
    calcitePlan
  }

}
