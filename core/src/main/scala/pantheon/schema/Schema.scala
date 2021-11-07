package pantheon.schema

import pantheon.planner._
import enumeratum._
import pantheon.{RecordQuery, AggregateQuery, PantheonQuery}
import pantheon.schema.Dimension._
import pantheon.schema.Schema.{QualifiedAttribute, QualifiedMeasure}
import pantheon.util.{BoolOps, toGroupedMap}

sealed trait Field {
  def name: String
  def metadata: ValueMap
}

object Dimension {
  type DimensionName = String
  type HierarchyName = String
  type LevelName = String
}

case class Dimension(name: String,
                     table: Option[String] = None,
                     metadata: ValueMap = Map(),
                     conforms: List[String] = List(),
                     hierarchies: List[Hierarchy] = List()) {

  def getAttribute(name: Array[String]): Option[(HierarchyName, LevelName, Attribute)] =
    (for {
      (hierarchyName, attrName) <- name.view.take(1).flatMap(nameHead => Seq(nameHead -> name.tail, "" -> name))
      hierarchy <- hierarchies.find(_.name == hierarchyName).toIterable
      (lvl, attr) <- hierarchy.getAttribute(attrName).toIterable
    } yield (hierarchyName, lvl, attr)).headOption

}

case class Hierarchy(name: String,
                     table: Option[String] = None,
                     metadata: ValueMap = Map(),
                     levels: List[Level] = List()) {

  def getAttribute(name: Array[String]): Option[(LevelName, Attribute)] =
    (for {
      (levelName, attrName) <- name.view.take(1).flatMap(nameHead => Seq(nameHead -> name.tail, "" -> name))
      level <- levels.find(_.name == levelName).toIterable
      attr <- level.getAttribute(attrName).toIterable
    } yield (levelName, attr)).headOption

}

case class Level(name: String,
                 table: Option[String] = None,
                 columns: List[String] = Nil,
                 expression: Option[String] = None,
                 castType: Option[String] = None,
                 metadata: ValueMap = Map(),
                 conforms: List[String] = Nil,
                 attributes: List[Attribute] = Nil) {

  def getAttribute(name: Array[String]): Option[Attribute] = {
    val attributeName = name.headOption.getOrElse("")
    attributes.find(_.name == attributeName)
  }

}

/* TODO: think of keeping table-field references in one place in compiled schema
   now everything is in the table, but there are additional optional fields on Attribute, Level etc..
   which the user of compiled schema probably does not ever need
 */
case class Attribute(name: String,
                     table: Option[String] = None,
                     columns: List[String] = Nil,
                     expression: Option[String] = None,
                     castType: Option[String] = None,
                     metadata: ValueMap = Map(),
                     conforms: List[String] = Nil)
    extends Field {}

sealed trait MeasureAggregate extends EnumEntry
object MeasureAggregate extends Enum[MeasureAggregate] {
  val values = findValues

  case object Sum extends MeasureAggregate
  case object Avg extends MeasureAggregate
  case object Count extends MeasureAggregate
  case object DistinctCount extends MeasureAggregate
  case object ApproxDistinctCount extends MeasureAggregate
  case object Max extends MeasureAggregate
  case object Min extends MeasureAggregate
}

trait MeasureAST {
  def name: String
  def metadata: ValueMap
}

case class CalculatedMeasureAST(
    name: String,
    calculation: NumericExpressionAST,
    metadata: ValueMap = Map()
) extends MeasureAST

case class FilteredMeasureAST(
    name: String,
    ref: String,
    filter: Predicate,
    metadata: ValueMap = Map()
) extends MeasureAST

case class AggMeasureAST(name: String,
                         aggregate: MeasureAggregate = MeasureAggregate.Sum,
                         columns: List[String],
                         filter: Option[Predicate] = None,
                         metadata: ValueMap = Map(),
                         conforms: List[String] = List())
    extends MeasureAST

sealed trait Measure extends Field {
  def name: String
  def metadata: ValueMap

  def copyCommon(
      name: String = name,
      metadata: ValueMap = metadata
  ) = {
    this match {
      case m: FilteredMeasure   => FilteredMeasure(name, m.base, m.filter, metadata)
      case m: AggMeasure        => AggMeasure(name, m.aggregate, m.columns, metadata, m.conforms)
      case m: CalculatedMeasure => CalculatedMeasure(name, m.calculation, metadata)
    }
  }
}

// calculated measure cannot reference another calculated measure for now (hence it is not an instance of NumericExpression)
case class CalculatedMeasure(
    name: String,
    calculation: NumericExpression,
    metadata: ValueMap = Map()
) extends Measure

case class FilteredMeasure(
    name: String,
    base: AggMeasure,
    filter: Predicate,
    metadata: ValueMap = Map()
) extends NumericExpression
    with Measure

case class AggMeasure(name: String,
                      aggregate: MeasureAggregate = MeasureAggregate.Sum,
                      columns: List[String] = Nil,
                      metadata: ValueMap = Map(),
                      conforms: List[String] = Nil)
    extends NumericExpression
    with Measure

case class AliasedName(name: String, alias: Option[String]) {
  assert(!name.contains("."))
}

case class SchemaAST(name: String,
                     dataSource: Option[String] = None,
                     strict: Boolean = true,
                     dimensions: List[Dimension] = List(),
                     measures: List[MeasureAST] = List(),
                     tables: List[TableAST] = List(),
                     imports: List[AliasedName] = List(),
                     exports: List[AliasedName] = List(),
                     defaultFilter: Option[Predicate] = None) {

  def getDimension(name: String): Option[Dimension] = dimensions.find(_.name == name)

  def getDimensionAttribute(name: String): Option[Attribute] = getDimensionAttribute(name.split("""\."""))

  def getMeasure(name: String): Option[MeasureAST] = measures.find(_.name == name)

  def getTable(name: String): Option[TableAST] = tables.find(_.name == name)

  def getTableAndColumn(tableRef: TableRef): Option[(TableAST, Column)] =
    getTable(tableRef.tableName).flatMap(t => t.columns.find(_.name == tableRef.colName).map((t, _)))

  /**
    * Find dimension attribute by name applying rules for name lookup.
    *
    * @param name Name can contain 2 or more components. First component is always dimension name.
    *             In each case we search names through the hierarchy of objects but
    *             some objects in hierarchy can have empty name. Than this object can be skipped. For example, name
    *             containing 2 components can match either default attribute for level or attribute in default level.
    * @return Dimension Attribute is successful, None - if not found
    */
  private def getDimensionAttribute(name: Array[String]): Option[Attribute] = {
    val len = name.length
    if (len < 2) None
    else {
      val dimAttr = for {
        n <- (len - 1 to 1 by -1).view
        dimName = name.slice(0, n).mkString(".")
        d <- dimensions if d.name == dimName
        (_, _, attr) <- d.getAttribute(name.slice(n, len))
      } yield attr
      dimAttr.headOption
    }
  }
}
object Compatibility {
  def getFields(c: Compatibility) = c.measures.map(_.reference) ++ c.dimensionAttributes.map(_.reference)
}
case class Compatibility(measures: Set[QualifiedMeasure], dimensionAttributes: Set[QualifiedAttribute])

object Schema {
  case class QualifiedAttribute(
      importedFromSchema: Option[String],
      dimensionName: String,
      hierarchyName: String,
      levelName: String,
      attribute: Attribute
  ) {
    def name = attribute.name
    val reference =
      (importedFromSchema.toSeq ++ Seq(dimensionName, hierarchyName, levelName, name).filter(_.nonEmpty))
        .mkString(".")
  }
  case class QualifiedMeasure(importedFromSchema: Option[String], measure: Measure) {
    val name = importedFromSchema.fold(reference)(reference.stripPrefix(_).stripPrefix("."))
    def reference = measure.name
  }
}
case class Schema(name: String,
                  strict: Boolean = true,
                  dimensions: List[Dimension] = List(),
                  measures: List[Measure] = List(),
                  tables: List[Table] = List(),
                  defaultFilter: Option[Predicate] = None,
                  excludes: Set[String] = Set()) {

  lazy val entityTables: Set[Table] = getEntityTables

  private lazy val dimensionConformance: Map[String, Set[String]] = buildDimensionConformance()

  private lazy val measureConformance: Map[String, Set[String]] = buildMeasureConformance()

  private lazy val originTables: Set[Table] = getOriginTables(tables.toSet)

  private lazy val originEntityTables: Set[Table] = getOriginTables(entityTables)

  // Discuss: rename to dimensionTables?
  private lazy val accessMap: Map[String, Set[String]] = getAccessMap(originTables)

  private lazy val entityAccessMap: Map[String, Set[String]] = getAccessMap(originEntityTables, entityTables)

  private lazy val measureTables: Map[String, Set[String]] = getMeasureTables

  // TODO: Move methods to SchemaOps
  def getTable(name: String): Option[Table] = tables.find(_.name == name)

  def getJoinDetails(tableRef: TableRef): Option[(Table, Column, JoinType.Value)] = {
    for {
      t <- getTable(tableRef.tableName)
      c <- t.columns.find(_.name == tableRef.colName)
    } yield (t, c, tableRef.joinType)
  }

  /**
    * Find dimension attribute by name applying rules for name lookup.
    *
    * @param name Name can contain 2 or more components. First component is always dimension name.
    *             In each case we search names through the hierarchy of objects but
    *             some objects in hierarchy can have empty name. Than this object can be skipped. For example, name
    *             containing 2 components can match either default attribute for level or attribute in default level.
    * @return Dimension Attribute is successful, None - if not found
    */
  private def getDimensionAttribute(name: Array[String]): Option[QualifiedAttribute] = {
    val len = name.length
    if (len < 2) None
    else {
      val dimAttr = for {
        n <- (len - 1 to 1 by -1).view
        schemaName = name.slice(0, n - 1).mkString(".")
        dimName = name.drop(n - 1).head
        dmension <- dimensions if dmension.name == (if (schemaName.isEmpty) dimName else schemaName + "." + dimName)
        (hierarchy, level, attr) <- dmension.getAttribute(name.slice(n, len)).toIterable
      } yield {
        QualifiedAttribute(
          (schemaName.nonEmpty).option(schemaName),
          dimName,
          hierarchy,
          level,
          attr
        )
      }
      dimAttr.headOption
    }
  }
  def foo[A](a: A = 42) = a

  def getDimension(name: String): Option[Dimension] = dimensions.find(_.name == name)

  def getDimensionAttribute(name: String): Option[Attribute] =
    getFullyQualifiedDimensionAttribute(name).map(_.attribute)

  def getFullyQualifiedDimensionAttribute(name: String): Option[QualifiedAttribute] =
    getDimensionAttribute(name.split("""\."""))

  def getMeasure(name: String): Option[Measure] = measures.find(_.name == name)
  def getFullyQualifiedMeasure(name: String): Option[QualifiedMeasure] =
    measures
      .find(_.name == name)
      .map { m =>
        val schemaName = name.take(name.lastIndexOf("."))
        QualifiedMeasure(schemaName.nonEmpty.option(schemaName), m)
      }

  def isExcluded(name: String): Boolean = excludes.contains(name.takeWhile(_ != '.'))

  def getField(name: String): Option[Field] =
    if (isExcluded(name)) None
    else getDimensionAttribute(name).orElse(getMeasure(name))

  // map built in reverse order in relation to "conforms" definitions
  private def buildDimensionConformance(): Map[String, Set[String]] = {
    val conformPairs = for {
      d <- dimensions
      h <- d.hierarchies
      l <- h.levels
      a <- l.attributes
      c <- a.conforms
    } yield (c, mkName(d.name, h.name, l.name, a.name))
    toGroupedMap(conformPairs)(_.toSet)
  }

  private def buildMeasureConformance(): Map[String, Set[String]] = {
    val conformPairs = for {
      m <- measures
      c <- m match {
        case m: AggMeasure        => m.conforms
        case _: FilteredMeasure   => Nil
        case _: CalculatedMeasure => Nil
      }
    } yield (c, m.name)
    toGroupedMap(conformPairs)(_.toSet)
  }

  private def getEntityTables: Set[Table] = {
    val allTables: Set[String] = dimensions.flatMap(
      d =>
        d.table ++
          d.hierarchies.flatMap(
            h =>
              h.table ++
                h.levels.flatMap(l =>
                  l.table ++
                    l.attributes.flatMap(_.table))))(collection.breakOut)

    tables.filter(t => allTables(t.name)).toSet
  }

  private def getOriginTables(tables: Set[Table]): Set[Table] =
    tables.filterNot(
      t =>
        tables.exists(
          _.columns.exists(
            _.tableRefs.exists(getJoinDetails(_).exists(_._1 == t))
          )))

  /**
    * Builds a map of table names to all accessible attributes
    * @return The map of table names to all accessible dimension attributes
    */
  private def getAccessMap(
      originTables: Set[Table],
      tableFilter: Table => Boolean = _ => true
  ): Map[String, Set[String]] =
    originTables.foldLeft(Map.empty[String, Set[String]]) { (am, t) =>
      buildAccessMap(t, am, tableFilter)
    }

  private def buildAccessMap(
      table: Table,
      accessMap: Map[String, Set[String]],
      tableFilter: Table => Boolean = _ => true
  ): Map[String, Set[String]] = {
    if (accessMap.contains(table.name)) accessMap
    else {
      val tables = table.columns.flatMap(_.tableRefs.flatMap(getJoinDetails(_).map(_._1))).toSet.filter(tableFilter)
      val newAccessMap = tables.foldLeft(accessMap)((am, t) => buildAccessMap(t, am))
      val access =
        tables.foldLeft(Set.empty[String])((a, t) => a ++ newAccessMap(t.name)) ++
          table.columns.flatMap(_.dimensionRefs.flatMap(getConformingAttributes))

      newAccessMap + ((table.name, access.filterNot(isExcluded)))
    }
  }

  private def getMeasureTables: Map[String, Set[String]] = {
    val measureTablePairs = for {
      t <- tables
      c <- t.columns
      measureRef <- c.measureRefs
      measure <- {
        val currentAndConforming = getConformingMeasures(measureRef) + measureRef
        currentAndConforming ++ measures.collect {
          case f: FilteredMeasure if currentAndConforming(f.base.name) => f.name
        }
      }
    } yield (measure, t.name)
    toGroupedMap(measureTablePairs)(_.toSet)
  }

  private def applyConstraints(names: Set[String], constraints: List[String]): Set[String] = {
    if (constraints.isEmpty) names
    else names.filter(n => constraints.exists(n.startsWith))
  }

  def getAggregateCompatibility(fields: Set[String], constraints: List[String] = Nil): Compatibility = {

    def withConstraints(dimensions: Set[String], measures: Set[String]): Compatibility =
      Compatibility(
        applyConstraints(measures, constraints).flatMap(getFullyQualifiedMeasure),
        applyConstraints(dimensions, constraints).flatMap(getFullyQualifiedDimensionAttribute)
      )

    // in dimensions: get union of that root table sets which contain all given dimensions
    def calcDims(tableNames: Set[String], selectedDims: Set[String]): Set[String] =
      tableNames
        .map(accessMap)
        .filter(selectedDims.subsetOf(_))
        .reduceOption(_ | _)
        .getOrElse(Set.empty)

    val calculatedMeasures = this.measures.collect { case m: CalculatedMeasure => m }

    if (fields.isEmpty) {
      // DISCUSS: could we replace measureTables.keySet with this.measures.map(_.name)  here? (given the fact that during compilation we ensure that every measure is referenced from column or conforms other measure)
      withConstraints(calcDims(originTables.map(_.name), Set.empty),
                      measureTables.keySet ++ calculatedMeasures.map(_.name))
    } else {
      val resolvedFields = fields.flatMap(f => getField(f).map(f -> _))
      //This situation may be resolved during compilation
      if (resolvedFields.size != fields.size) Compatibility(Set.empty, Set.empty)
      else {

        val selectedMeasures = resolvedFields.collect {
          case (_, m: Measure) => m
        }

        val selectedDimAttributes = resolvedFields.collect {
          case (n, _: Attribute) => n
        }

        def getDimensionAttrNames(m: Measure): List[String] = {
          m match {
            case f: FilteredMeasure =>
              f.filter.collect {
                case UnresolvedField(name) =>
                  getField(name) match {
                    case Some(_: Measure) =>
                      throw new AssertionError(
                        "Measure is not allowed in filter(should be validated during compilation)")
                    case Some(a: Attribute) => name
                    case None =>
                      throw new AssertionError(
                        "Filter refers to field that is not found in schema(should be validated during compilation)")
                  }
              }
            case _: AggMeasure        => Nil
            case _: CalculatedMeasure => Nil
          }
        }

        val dims = {

          if (selectedMeasures.isEmpty) calcDims(originTables.map(_.name), selectedDimAttributes)
          else {
            // in dimensions: return intersection of (measures -> union of all measure table's
            // dimension sets which contain all given dimensions)
            def getDims(m: Measure): Set[String] = m match {
              case m: CalculatedMeasure =>
                val ms = NumericExpression.collectMeasures(m.calculation)
                ms.map(m => getDims(m.merge)).reduceLeftOption(_ & _).getOrElse(Set.empty)
              case m: FilteredMeasure =>
                calcDims(measureTables(m.name), selectedDimAttributes ++ getDimensionAttrNames(m))
              case m: AggMeasure =>
                calcDims(measureTables(m.name), selectedDimAttributes)
            }

            selectedMeasures.map(getDims).reduceOption(_ & _).getOrElse(Set.empty)
          }
        }
        // in measures: return all measures that contain all specified dimensions queried from one of the measure tables
        //TODO: better name
        val measures: Set[String] =
          (for {
            (measureName, tables) <- measureTables.filterKeys(v => !isExcluded(v))
            measure = getMeasure(measureName)
              .ensuring(_.isDefined, "all measures listed in measureTables must be present in schema")
              .get
            attrsFromMeasure = getDimensionAttrNames(measure)
            if tables.exists(t => (selectedDimAttributes ++ attrsFromMeasure).subsetOf(accessMap(t)))
          } yield measureName)(collection.breakOut)

        val availbaleCalMeasures =
          calculatedMeasures
            .withFilter(cm => NumericExpression.collectMeasures(cm.calculation).forall(m => measures(m.merge.name)))
            .map(_.name)
        withConstraints(dims, measures ++ availbaleCalMeasures)
      }
    }
  }

  def getEntityCompatibility(fields: Set[String], constraints: List[String] = Nil): Compatibility = {
    val dims: Set[String] =
      originEntityTables
        .map(t => entityAccessMap(t.name))
        .filter(fields.subsetOf(_))
        .reduceOption(_ | _)
        .getOrElse(Set.empty)

    Compatibility(Set.empty, applyConstraints(dims, constraints).flatMap(getFullyQualifiedDimensionAttribute))
  }

  def getCompatibility(
      q: PantheonQuery,
      constraints: List[String] = Nil
  ): Compatibility = {

    val (identFlds, entity) = q match {
      case q: AggregateQuery =>
        (if (q.pivoted) q.columns ::: q.measures else q.measures) -> false
      case q: RecordQuery => Nil -> true
    }

    val allFlds: Set[String] =
      q.filter.toSet[Predicate].flatMap(_.collect { case f: FieldRef => f.name }) ++ q.rows ++ identFlds

    if (entity) getEntityCompatibility(allFlds, constraints)
    else getAggregateCompatibility(allFlds, constraints)
  }

  private def getConformingMeasures(measure: String): Set[String] =
    Set(measure) ++ measureConformance
      .get(measure)
      .map(_.map(getConformingMeasures).reduceLeft(_ ++ _))
      .getOrElse(Set.empty)

  private def getConformingAttributes(attr: String): Set[String] =
    Set(attr) ++ dimensionConformance
      .get(attr)
      .map(_.map(getConformingAttributes).reduceLeft(_ ++ _))
      .getOrElse(Set.empty)

  /**
    * Determines if an attribute is accessible from the table by join paths
    * @param table table from which check accessibility
    * @param attribute attribute to check for accessibility
    * @return is attribute accessible or not
    */
  def isDerivable(table: Table, attribute: String): Boolean = accessMap(table.name).contains(attribute)

  /**
    * Determines if an attribute is accessible from the table by join paths between entity tables
    * @param table table from which check accessibility
    * @param attribute attribute to check for accessibility
    * @return is attribute accessible or not
    */
  def isEntityDerivable(table: Table, attribute: String): Boolean = entityAccessMap(table.name).contains(attribute)

  /**
    * Checks for measure conformance recursively
    * Note that conformance is a directed relationship
    */
  def measureConformsTo(measure: String, to: String): Boolean =
    measure == to || measureConformance.get(to).exists(_.exists(measureConformsTo(measure, _)))

  /**
    * Determines if the measure exists in the table (taking into account conformance relationship)
    * @param table table to check for measure (that is, measure reference)
    * @param measure measure to check for
    * @return true - if table contains given measure
    */
  def tableContainsMeasure(table: Table, measure: String): Boolean = getMeasureColumn(table, measure).nonEmpty

  /**
    * Gets table column which contain measure conforming to given measure
    * @param table table to find column in
    * @param measure measure that shall conform to the column's measure
    * @return column if found
    */
  def getMeasureColumn(table: Table, measure: String): Option[Column] =
    table.columns.find(col => col.measureRefs.exists(measureConformsTo(measure, _)))

  /**
    * Checks for attribute conformance recursively
    * Note that conformance is a directed relationship
    */
  def attributeConformsTo(attribute: String, to: String): Boolean =
    attribute == to || dimensionConformance.get(to).exists(_.exists(attributeConformsTo(attribute, _)))

  /**
    * Determines if dimension attribute exists in the table (taking into account conformance relationship)
    * @param table table to check for attribute existence
    * @param attribute attribute to check for
    * @return true - if attribute exists in the table
    */
  def tableContainsAttribute(table: Table, attribute: String): Boolean =
    getDimensionColumn(table, attribute).nonEmpty

  /**
    * Gets table column which contains attribute conforming to the attribute given as a parameter
    * @param table table to find column in
    * @param attribute dimension attribute that shall conform to the column's dimension attribute
    * @return column if found
    */
  def getDimensionColumn(table: Table, attribute: String): Option[Column] =
    table.columns.find(col => col.dimensionRefs.exists(attributeConformsTo(attribute, _)))

}
