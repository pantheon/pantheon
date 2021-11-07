package pantheon.planner

import pantheon.schema._

import scala.language.higherKinds
import pantheon.util.Logging.{ContextLogging, LoggingContext}

class PlannerException(message: String, cause: Throwable = null) extends Exception(message, cause)

object Planner {
  type Rule = QueryPlan => QueryPlan

}

/**
  * Planner proceeds through the following steps to execute the query:
  * 1. Resolve all unresolved entities like schemas, dimensions, measures
  * 2. Find all tables that can be used to satisfy the query with required granularity
  * (all necessary dimension attributes shall be accessible, measures can be spread by several fact tables,
  * in this case results will be joined together after pre-aggregation)
  * 3. Find the combination with the lowest cost (by the number of joined tables or what?)
  *
  * @param schema
  */
class Planner(_schema: Schema) extends ContextLogging {

  import Planner._

  def execute(root: QueryPlan, rules: Seq[Rule])(implicit ctx: LoggingContext): QueryPlan = rules.foldLeft(root) {
    case (query, rule) =>
      val transformed = rule(query)
      logger.debug(s"After applying ${rule.getClass.getName} the plan is:\n ${QueryPlan.pprint(transformed)}")
      transformed
  }

  // List of rules for multi dimensional query
  val rules: List[Rule] =
    ResolveSchema ::
      ResolveOutputFields ::
      ResolveFields ::
      FindSuitableTables ::
      ChooseOptimalCombination ::
      ConvertFilter ::
      ConvertAggregate ::
      OptimizeAggregate ::
      ConvertProject ::
      ConvertExpressionsToView ::
      ConvertSuperJoin ::
      Nil

  object ResolveSchema extends Rule {
    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case _: UnresolvedSchema =>
        ResolvedSchema(_schema)
    }
  }

  private def resolveFilter(p: Predicate,
                            schema: Schema): (Predicate, Set[ResolvedDimensionAttribute], Set[ResolvedMeasure]) = {
    var dimensionAttributes: Set[ResolvedDimensionAttribute] = Set()
    var measures: Set[ResolvedMeasure] = Set()
    val cond = p transformUp {
      case UnresolvedField(name) =>
        val field = resolveField(name, schema)

        field match {
          case a: ResolvedDimensionAttribute =>
            dimensionAttributes += a
            a
          case m: ResolvedMeasure =>
            measures += m
            m
          case m: ResolvedCalculatedMeasure =>
            throw new PlannerException(s"Calculations are not supported in filters ($name)")
          case uf: UnresolvedField => throw new PlannerException(s"Unexpected type in filter $uf")
        }
    }
    (cond.asInstanceOf[Predicate], dimensionAttributes, measures)
  }

  private def resolveAggMes(m: AggMeasure): ResolvedMeasure = ResolvedMeasure(m.name, Left(m), Set.empty)

  private def resolveFilteredMes(m: FilteredMeasure, schema: Schema): ResolvedMeasure = {
    val (newFilter, dimAttrs, measures) = resolveFilter(m.filter, schema)
    if (measures.nonEmpty) throw new PlannerException(s"Measure filter cannot contain another measure (${m.name})")
    ResolvedMeasure(m.name, Right(m.copy(filter = newFilter)), dimAttrs)
  }

  private def findSchema(root: QueryPlan): Schema = {
    root.collectFirst {
      case ResolvedSchema(s, _, _, _) => s
    } getOrElse (throw new PlannerException("Underlying schema could not be found for node"))
  }

  private def resolveField(name: String, schema: Schema): FieldRef =
    schema.getField(name).getOrElse(throw new PlannerException(s"Field '$name' cannot be found")) match {
      case m: AggMeasure      => resolveAggMes(m)
      case m: FilteredMeasure => resolveFilteredMes(m, schema)
      case a: Attribute       => ResolvedDimensionAttribute(name, a)
      case m: CalculatedMeasure =>
        ResolvedCalculatedMeasure(
          m.name,
          m.calculation,
          NumericExpression
            .collectMeasures(m.calculation)
            .map(_.fold(resolveAggMes, fm => resolveFilteredMes(fm, schema)))
        )
    }

  private def hasAggregate(root: QueryPlan): Boolean =
    root.collectFirst { case v: SchemaAggregate => v }.nonEmpty

  object ResolveOutputFields extends Rule {

    override def apply(root: QueryPlan): QueryPlan = {

      root transformUp {

        case SchemaProject(fields, child) =>
          val schema = findSchema(child)

          val newFields = fields.map {
            case UnresolvedField(name) => resolveField(name, schema)
            case f: FieldRef           => throw new AssertionError(s"Unexpected resolved field in Project $f")
          }

          val resolvedMeasures = newFields.collect { case v: ResolvedMeasure               => v }
          val resolvedCalculations = newFields.collect { case v: ResolvedCalculatedMeasure => v }
          val resolvedDimAttrs = newFields.collect { case v: ResolvedDimensionAttribute    => v }

          val additionalMeasures = resolvedCalculations.flatMap(_.requiredMeasures)

          if (!hasAggregate(child)) {
            // we have non-aggregated (entity) query
            if (resolvedMeasures.nonEmpty || resolvedCalculations.nonEmpty)
              throw new PlannerException("Measures are not allowed in non-aggregated query")
          }

          val newChild = child transformUp {
            case a: SchemaAggregate =>
              a.copy(
                outputDimensionAttributes = (a.outputDimensionAttributes ++ resolvedDimAttrs).distinct,
                outputMeasures = (a.outputMeasures ++ resolvedMeasures ++ additionalMeasures).distinct
              )
            case s: ResolvedSchema =>
              s.copy(
                usedDimensionAttributes = (s.usedDimensionAttributes ++ resolvedDimAttrs).distinct,
                outputDimensionAttributes = (s.outputDimensionAttributes ++ resolvedDimAttrs).distinct,
                outputMeasures = (s.outputMeasures ++ resolvedMeasures ++ additionalMeasures).distinct
              )
          }

          SchemaProject(newFields, newChild)
      }
    }
  }

  object ResolveFields extends Rule {

    def addDimensionsMeasures(root: QueryPlan,
                              dimensionAttributes: Set[ResolvedDimensionAttribute],
                              measures: Set[ResolvedMeasure],
                              addDimensionsToOutput: Boolean = true): QueryPlan =
      if (hasAggregate(root)) {
        root.transformUp {
          case s @ ResolvedSchema(_, _, _, ms) =>
            s.copy(outputMeasures = ms ++ measures)
          case a @ SchemaAggregate(dims, ms, _) =>
            dimensionAttributes.foreach { da =>
              if (!dims.contains(da))
                throw new PlannerException(
                  s"After aggregation all dimension attributes should be from output list (${da.name})")
            }
            a.copy(outputMeasures = (ms ++ measures).distinct)
        }
      } else {
        if (measures.nonEmpty) throw new PlannerException("Measures are not allowed without aggregation")
        root.transformUp {
          case s @ ResolvedSchema(_, usedDims, outDims, ms) =>
            s.copy(
              usedDimensionAttributes = (usedDims ++ dimensionAttributes).distinct,
              outputDimensionAttributes =
                if (addDimensionsToOutput) (outDims ++ dimensionAttributes).distinct else outDims
            )
        }
      }

    override def apply(root: QueryPlan): QueryPlan = {

      root transformUp {

        case Filter(condition, child) =>
          val (newCondition, dimensionAttributes, measures) = resolveFilter(condition, findSchema(child))
          Filter(newCondition,
                 addDimensionsMeasures(child, dimensionAttributes, measures, addDimensionsToOutput = false))

        case Sort(columns, child) =>
          val schema = findSchema(child)

          // resolving dims/measures, updating OrderCOlums, preserving ordering
          val newCols = columns.collect {
            case c @ OrderedColumn(UnresolvedField(name), _) => c.copy(ref = resolveField(name, schema))
          }

          val dimensionAttributes = newCols.collect {
            case OrderedColumn(a: ResolvedDimensionAttribute, _) => a
          }.toSet

          val measures = newCols.collect {
            case OrderedColumn(m: ResolvedMeasure, _) => m
          }.toSet

          Sort(newCols, addDimensionsMeasures(child, dimensionAttributes, measures))
      }
    }

  }

  /**
    * Finds all starting tables that can be used to fulfil the query.
    * Starting table is a table which:
    *   - contains at least one measure if measures are not empty
    *   - every required dimension attribute can be accessed by joins from this table
    */
  object FindSuitableTables extends Rule {

    def buildSuitableTables(rs: ResolvedSchema) = {

      import rs.{usedDimensionAttributes => attributes, outputMeasures => measures}

      def nonEmpty[I[T] <: Iterable[T]](msg: String)(s: I[Table]): I[Table] =
        if (s.isEmpty) throw new PlannerException(msg) else s

      val suitableTables: List[(Table, Set[ResolvedMeasure])] =
        if (measures.nonEmpty) {
          val isSuitable = measures
            .flatMap(
              mes =>
                nonEmpty(s"Cannot find tables for measure ${mes.name}")(
                  rs.schema.tables
                    .filter(table =>
                      rs.schema.tableContainsMeasure(table, mes.baseName) &&
                        (attributes ++ mes.requiredDimAttrs).forall(attr => rs.schema.isDerivable(table, attr.name))))
                  .map(t => mes -> t)
            )
            .groupBy(_._2)
            .mapValues(_.map(_._1).toSet)
          //tables order is preserved to make plans compatible with existing expected plans in tests
          rs.schema.tables.withFilter(isSuitable.contains).map(t => t -> isSuitable(t))
        } else if (attributes.nonEmpty) {
          nonEmpty("cannot find tables covering all dimension attributes of dimension-only query")(
            rs.schema.tables.filter(
              t =>
                attributes.exists(a => rs.schema.tableContainsAttribute(t, a.name)) &&
                  attributes.forall(a => rs.schema.isDerivable(t, a.name)))
          ).map(_ -> Set.empty[ResolvedMeasure])
        } else throw new PlannerException("Measures and attributes are empty, nothing to query")

      SuitableTables(
        rs.schema,
        attributes,
        rs.outputDimensionAttributes,
        measures,
        suitableTables
      )
    }

    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case rs: ResolvedSchema => buildSuitableTables(rs)
    }
  }

  def findColumnByAttribute(resolvedAttribute: ResolvedDimensionAttribute, root: QueryPlan): ColumnRef = {
    root
      .collectFirst {
        case ts @ TableScan(schema, table, attributes, _) if attributes.contains(resolvedAttribute) =>
          schema
            .getDimensionColumn(table, resolvedAttribute.name)
            .map(c => ColumnRef(c.name, Some(table.name)))
            .getOrElse(throw new PlannerException("Cannot find table column containing dimension attribute"))
      }
      .getOrElse(throw new PlannerException("Cannot find TableScan containing dimension attribute"))
  }

  /**
    * Iterates through possible combinations of starting tables and find optimal by the total number of tables used
    * which is equal to the total number of joins. Then substitutes SuitableTables with a number of Joins
    */
  object ChooseOptimalCombination extends Rule {

    /**
      * Builds join tree starting from the given table that accommodates all given attributes and measures
      *
      * @param schema     used schema
      * @param table      table to start join tree from
      * @param attributes necessary dimension attributes
      * @param measures   necessary measures
      * @return tuple of join QueryPlan and the set of attributes resolved by this join
      */
    // Note: this can be optimised by MST algorithm
    def buildJoins(schema: Schema,
                   table: Table,
                   attributes: Set[ResolvedDimensionAttribute],
                   measures: Set[ResolvedMeasure],
                   tableFilter: Table => Boolean): (QueryPlan, Set[ResolvedDimensionAttribute]) = {
      val (tableResolvedAttrs, tableRemainingAttrs) =
        attributes.partition(a => schema.tableContainsAttribute(table, a.name))
      val tableScan = TableScan(schema, table, tableResolvedAttrs, measures)
      if (tableRemainingAttrs.isEmpty) (tableScan, tableResolvedAttrs) // all required attributes exist in this table
      else {
        val colTableRefs = for {
          column <- table.columns
          tableRef <- column.tableRefs
          (rightTable, rightCol, joinType) <- schema.getJoinDetails(tableRef) if tableFilter(rightTable)
        } yield (column, rightTable, rightCol, joinType)

        colTableRefs.foldLeft((tableScan.asInstanceOf[QueryPlan], tableResolvedAttrs)) { (left, right) =>
          val (leftChild, leftResolvedAttrs) = left
          val (leftCol, rightTable, rightCol, joinType) = right
          val (rightChild, rightResolvedAttrs) =
            buildJoins(schema, rightTable, tableRemainingAttrs -- leftResolvedAttrs, Set(), tableFilter)
          if (rightResolvedAttrs.nonEmpty)
            (Join(leftChild,
                  rightChild,
                  ColumnRef(leftCol.name, Some(table.name)),
                  ColumnRef(rightCol.name, Some(rightTable.name)),
                  joinType),
             leftResolvedAttrs ++ rightResolvedAttrs)
          else left
        }
      }
    }

    def containsAllMeasures(schema: Schema,
                            tables: Seq[(Table, Set[ResolvedMeasure])],
                            measures: List[ResolvedMeasure]): Boolean =
      measures.forall(m => tables.exists(_._2.contains(m)))

    def findColumnsByMeasure(resolvedMeasure: ResolvedMeasure, root: QueryPlan): List[NamedExpression] = {
      val measureBaseName = resolvedMeasure.baseName

      val col = root
        .collectFirst({
          case TableScan(schema, table, _, measures) if measures.contains(resolvedMeasure) =>
            schema
              .getMeasureColumn(table, measureBaseName)
              .map(c => NamedExpression(measureBaseName, ColumnRef(c.name, Some(table.name))))
              .getOrElse(throw new PlannerException("Cannot find table column containing measure"))
        })
        .getOrElse(throw new PlannerException("Cannot find TableScan containing measure"))

      val measuresFilterColumns =
        resolvedMeasure.requiredDimAttrs.map(a => NamedExpression(a.name, findColumnByAttribute(a, root))).toList

      col :: measuresFilterColumns
    }

    /**
      * Builds join on conforming dimensions.
      * The name SuperJoin is derived from the common structure of query plans: SuperJoin joins already aggregated results
      * from different fact tables.
      *
      * @param schema     used schema
      * @param tables     sequence of starting tables to generate superjoin with
      * @param attributes necessary dimension attributes
      * @param measures   necessary measures
      * @return
      */
    def buildSuperJoin(schema: Schema,
                       tables: Seq[(Table, Set[ResolvedMeasure])],
                       attributes: List[ResolvedDimensionAttribute],
                       outAttributes: List[ResolvedDimensionAttribute],
                       measures: List[ResolvedMeasure],
                       tableFilter: Table => Boolean = { _ =>
                         true
                       }): SuperJoin = {
      tables.foldRight(SuperJoin(Nil, attributes, outAttributes, Nil)) { (tm, sj) =>
        val (table, tableMeasures) = tm
        val measuresToQuery = tableMeasures -- sj.outputMeasures.flatten
        val measureAttributes = measuresToQuery.flatMap(_.requiredDimAttrs)
        val attributesToQuery = attributes.toSet ++ measureAttributes
        val (queryPlan, _) = buildJoins(schema, table, attributesToQuery, measuresToQuery, tableFilter)

        val outputColumns =
          (outAttributes ++ measureAttributes.toList).distinct.map(a =>
            NamedExpression(a.name, findColumnByAttribute(a, queryPlan))) ++
            measuresToQuery.flatMap(findColumnsByMeasure(_, queryPlan))

        SuperJoin(Project(outputColumns.distinct, queryPlan) :: sj.plans,
                  attributes,
                  outAttributes,
                  measuresToQuery :: sj.outputMeasures)
      }
    }

    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case SuitableTables(schema, usedAttributes, outAttributes, measures, tables) =>
        val maxTables = (measures.size min tables.size) max 1
        val tableFilter: Table => Boolean = if (measures.isEmpty) schema.entityTables else _ => true
        (1 to maxTables).view
          .flatMap(n =>
            tables.combinations(n).foldLeft[Option[QueryPlan]](None) { (join, tableCombination) =>
              if (containsAllMeasures(schema, tableCombination, measures)) {
                val newJoin =
                  buildSuperJoin(schema, tableCombination, usedAttributes, outAttributes, measures, tableFilter)
                if (newJoin.size < join.fold(Int.MaxValue)(_.size)) Some(newJoin) else join
              } else join
          })
          .headOption
          .getOrElse(throw new AssertionError("Cannot build SuperJoin"))
    }
  }

  def resolveColumns[T <: QueryPlan](f: T): T =
    f.transformUp {
        case a: FieldRef => ColumnRef(a.name)
      }
      .asInstanceOf[T]

  object ConvertFilter extends Rule {

    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case f @ Filter(condition, child) =>
        if (hasAggregate(child)) {
          // this is post-aggregate filter
          f.copy(condition = resolveColumns(condition))
        } else {
          // push filter into SuperJoin
          child transformUp {
            case sj: SuperJoin =>
              sj.copy(plans = sj.plans.map(_.transformUp {
                case p: Project =>
                  val cond = condition
                    .transformUp {
                      case a: ResolvedDimensionAttribute => findColumnByAttribute(a, p.child)
                    }
                    .asInstanceOf[Predicate]
                  p.copy(child = Filter(cond, p.child))
              }))
          }
        }
    }
  }

  /**
    * Adds aggregate node at the top of each tabular project inside SuperJoin
    */
  object ConvertAggregate extends Rule {
    override def apply(root: QueryPlan): QueryPlan = {
      root transformUp {
        case SchemaAggregate(dimensionAttributes, measures, child) =>
          child transformUp {
            case sj: SuperJoin =>
              sj.copy(plans = sj.plans.zip(sj.outputMeasures).map {
                case (p, ms) =>
                  val groupingColumns = dimensionAttributes.map(a => ColumnRef(a.name))
                  val aggColumns = measures.collect {
                    case m: ResolvedMeasure if ms.contains(m) =>
                      m.measure.fold(
                        a => Aggregation(ColumnRef(a.name), a.name, a.aggregate, None),
                        f =>
                          Aggregation(ColumnRef(f.base.name), f.name, f.base.aggregate, Some(resolveColumns(f.filter)))
                      )
                  }

                  Aggregate(groupingColumns, aggColumns, p)
              })
          }
      }

    }
  }

  object OptimizeAggregate extends Rule {
    override def apply(root: QueryPlan): QueryPlan = {
      root transformUp {
        case a @ Aggregate(groupColumns, aggregations, child) =>
          if (aggregations.nonEmpty) {
            val f = aggregations.head.filter
            if (f.nonEmpty && aggregations.tail.forall(_.filter == f)) {
              Aggregate(groupColumns, aggregations.map(_.copy(filter = None)), Filter(f.get, child))
            } else a
          } else a
      }
    }
  }

  object ConvertProject extends Rule {
    def convertNumericExpression(n: NumericExpression): Expression =
      n.transformDown {
          case m: Measure => ColumnRef(m.name)
        }
        .asInstanceOf[Expression]

    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case SchemaProject(fields, child) =>
        val columns = fields collect {
          case ResolvedCalculatedMeasure(name, expression, _) =>
            NamedExpression(name, convertNumericExpression(expression))
          case ResolvedDimensionAttribute(name, dimensionAttribute) =>
            val col = ColumnRef(name)
            NamedExpression(name, dimensionAttribute.castType.fold[Expression](col)(t => Cast(t, col)))
          case f: FieldRef =>
            NamedExpression(f.name, ColumnRef(f.name))
        }
        Project(columns, child)
    }
  }

  object ConvertExpressionsToView extends Rule {
    override def apply(root: QueryPlan): QueryPlan = root transformUp {
      case ts @ TableScan(schema, table, outputDimensionAttributes, outputMeasures) =>
        val dimColumns = outputDimensionAttributes.map(a => schema.getDimensionColumn(table, a.name).get)
        val measureColumns =
          outputMeasures.map(m => schema.getMeasureColumn(table, m.baseName).get)
        val expressionColumns = (dimColumns ++ measureColumns).filter(_.expression.nonEmpty)
        if (expressionColumns.isEmpty && table.definition.isLeft) ts
        else {
          View(table, expressionColumns.toList.map(c => ViewExpression(c.name, c.expression.get)))
        }
    }
  }

  /**
    * Converts SuperJoin to a number of joins
    */
  object ConvertSuperJoin extends Rule {

    override def apply(root: QueryPlan): QueryPlan = root transformDown {
      case SuperJoin(plans, _, attributes, _) =>
        val columnNames = attributes.map(_.name)
        plans.reduceLeft { (l, r) =>
          JoinUsing(l, r, columnNames)
        }
    }

  }

}
