package pantheon.schema

import scala.language.higherKinds
import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import pantheon.{Catalog, DataSource}
import pantheon.planner._
import pantheon.util.{duplicates, joinUsing}

import scala.annotation.tailrec
import cats.instances.list._
import cats.instances.option._
import cats.instances.either._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.option._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.alternative._
import pantheon.util.BoolOps
import pantheon.planner.literal.{Literal, NumericLiteral}
import Compatibility.getFields
import reflect.internal.util.Collections.flatCollect
import scala.reflect.internal.util.Collections.distinctBy

case class SchemaCompilationError(message: String, cause: Throwable = null) extends Exception(message, cause)

object SchemaCompiler {

  def compile(catalog: Catalog, s: SchemaAST): Either[List[SchemaCompilationError], Schema] = {
    val allDataSources = catalog.getDatasourcesForSchema(s.name)
    val isReferenced: Set[String] = (s.tables.flatMap(_.dataSource)(collection.breakOut): Set[String]) ++ s.dataSource
    compilePure(
      s,
      (s.imports ::: s.exports).map(_.name).distinct.flatMap(catalog.getSchema),
      allDataSources.filter(ds => isReferenced(ds.name))
    )
  }

  // allows finding of dependencies with arbitrary effects(i.e. DBIO, Future, Id(no effects))
  def compileParsed[M[_]: Monad](
      psl: SchemaAST,
      findSchema: String => M[Option[SchemaAST]],
      findDataSource: String => M[DataSource]
  ): M[Either[List[SchemaCompilationError], Schema]] = {

    def compileUsed(name: String): M[Option[Schema]] = {
      findSchema(name).flatMap(
        _.traverse(
          v =>
            compileParsed(v, findSchema, findDataSource).map(
              _.fold(
                // Imports/exports are expected to refer to existing (fully validated) schemas.
                e => throw new AssertionError(s"imported/exported Schema $name failed to compile:" + e),
                identity
              )
          ))
      )
    }

    for {
      usedSchemas <- (psl.imports ::: psl.exports).map(_.name).distinct.traverse(compileUsed)
      dataSources <- (psl.tables.flatMap(_.dataSource) ++ psl.dataSource).distinct.traverse(findDataSource)
    } yield compilePure(psl, usedSchemas.flatten, dataSources)
  }

  // main compilation method: compiles schema with all dependencies resolved
  def compilePure(
      s: SchemaAST,
      usedSchemas: Seq[Schema],
      dataSources: Seq[DataSource]
  ): Either[List[SchemaCompilationError], Schema] = {

    val dims = addDefaultAttributes(s.dimensions)

    val r: Either[NonEmptyList[String], Schema] = for {
      schemaDataSource <- s.dataSource.traverse(dsName =>
        dataSources.find(_.name == dsName).toRight(NonEmptyList.of(s"schema data source '$dsName' not found")))

      _ <- checkImports(s.imports, s.exports).map(NonEmptyList.one).toLeft(())

      importedSchemas <- (s.imports ::: s.exports).traverse(importSchema(usedSchemas, _)).toEither

      importedMeasures = importedSchemas.flatMap(_.measures)

      measures <- compileMeasures(s.measures, importedMeasures)

      tables <- s.tables.traverse(compileTables(_, schemaDataSource, dataSources).toValidatedNel).toEither

      localAndImportedTables = tables ::: importedSchemas.flatMap(_.tables)

      inferredColumnsFromRefs = inferColumnsFromRefs(localAndImportedTables, dims, measures)

      allTables <- if (s.strict)
        Right(
          localAndImportedTables
            .map(
              t =>
                if (t.addAllDBColumns) addColumns(t, inferredColumnsFromRefs)
                else t)
        )
      else
        getAdditionalTables(dims, localAndImportedTables, schemaDataSource)
          .bimap(
            NonEmptyList.one,
            additionalTables => {
              val allTables = additionalTables ++ localAndImportedTables
              assert(
                (inferredColumnsFromRefs.keySet -- allTables.map(_.name)).isEmpty,
                "all missing table references must be inferred at this point"
              )
              allTables.map(addColumns(_, inferredColumnsFromRefs))
            }
          )

      merged = Schema(
        s.name,
        s.strict,
        dims ++ importedSchemas.flatMap(_.dimensions),
        measures ++ importedMeasures,
        enrichWithRefs(allTables, inferredColumnsFromRefs),
        /*not merging default filter because:
         * 1)fields of default filter from imported schema need to be exported (in current impl default filer is merged with query filter before query execution)
         * 2)it introduces additional joins even if fields of imported schema are not used
         * 3)it can cause the incompatibility for all queries (if imported filter and existing filter have incompatible set of fields)
         */
        s.defaultFilter,
        (s.imports.flatMap(importToExcludes(importedSchemas, _)) ++
          importedSchemas.flatMap(_.excludes))(collection.breakOut)
      )

      dp <- processConformingDimensions(merged).left.map(NonEmptyList.one)

      _ <- NonEmptyList.fromList(validate(dp)).toLeft(())
    } yield new Schema(dp.name, s.strict, dp.dimensions, dp.measures, dp.tables, dp.defaultFilter, dp.excludes)

    r.left.map(_.toList.map(SchemaCompilationError(_)))
  }

  /**
    * Checks that there is no duplicates in imports/exports
    * @param imports
    * @param exports
    * @return Some(String) with description of error in case of error, None - if success
    */
  def checkImports(imports: List[AliasedName], exports: List[AliasedName]): Option[String] = {
    val allImportNames = (imports ++ exports).map(_.name)
    val duplicates = allImportNames.diff(allImportNames.distinct)
    // there is no point in importing/exporting schemas multiple times. This will lead to confusions.
    duplicates.nonEmpty.option(s"schemas ${duplicates.mkString("[", ",", "]")} are imported/exported more than once")
  }

  def importToExcludes(importedSchemas: Seq[Schema], i: AliasedName): Seq[String] =
    i.alias match {
      // wildcard case
      case None =>
        importedSchemas
          .find(_.name == i.name)
          .toSeq
          .flatMap(s => s.measures.map(_.name) ++ s.dimensions.map(_.name) ++ s.tables.map(_.name))

      case Some(alias) => Seq(alias)
    }

  private def importSchema(
      usedSchemas: Seq[Schema],
      importOrExport: AliasedName
  ): ValidatedNel[String, Schema] = {
    val schema = usedSchemas.find(_.name == importOrExport.name).toValidNel(s"schema ${importOrExport.name} not found")
    importOrExport.alias.fold(schema)(alias => schema.map(withAlias(alias, _)))
  }

  private def readColumnRef(columnRef: String): (Option[String], String) = {
    val idx = columnRef.lastIndexOf(".")
    if (idx == -1) (None, columnRef)
    else {
      val (tableName, colName) = columnRef.splitAt(idx)
      (Some(tableName), colName.stripPrefix("."))
    }
  }

  /**
    * Validates the schema structurally and semantically.
    * Note that it makes sense to validate already normalised schema to ensure that all necessary entities already exist
    */
  private def validate(schema: Schema): List[String] = {
    def refError(message: String, table: Table, column: Column) =
      s"$message at table: '${table.name}', column: '${column.name}'"

    val duplicateDimMesErrors =
      duplicates(schema.dimensions.map(_.name) ++ schema.measures.map(_.name))
        .map(n => s"Ambiguous dimension/measure name: '$n'")

    val duplicateTableErrors =
      duplicates(schema.tables.map(_.name))
        .map(n => s"Ambiguous table name: '$n'")

    val columnDuplicateErrors = schema.tables.flatMap { t =>
      val dups = duplicates(t.columns.map(_.name))
      dups.nonEmpty.option(s"table '${t.name}' contains columns with duplicate names ${dups.mkString("[", ",", "]")}")
    }

    def columnExists(table: Option[String], column: String): Boolean =
      table.flatMap(t => schema.getTable(t)).exists(_.getColumn(column).nonEmpty)

    val brokenMeasureColumns: List[String] =
      for {
        measure <- schema.measures.collect {
          case a: AggMeasure      => a
          case f: FilteredMeasure => f.base
        }
        column <- measure.columns if !(columnExists _).tupled(readColumnRef(column))
      } yield s"Measure ${measure.name} referencing non-existent table column $column"

    val brokenDimensionColumns: List[String] =
      for {
        dim <- schema.dimensions
        h <- dim.hierarchies
        l <- h.levels
        a <- l.attributes
        column <- a.columns
        (cTable, col) = readColumnRef(column)
        table = cTable.orElse(a.table).orElse(l.table).orElse(h.table).orElse(dim.table)
        errorMsg <- if (columnExists(table, col)) None
        else
          Some(s"Attribute ${mkName(dim.name, h.name, l.name, a.name)} referencing non-existent table column $column")
      } yield errorMsg

    val brokenTableRefErrs: List[String] = (for {
      table <- schema.tables
      column <- table.columns
      dimensionRefErrors = column.dimensionRefs
        .filter(schema.getDimensionAttribute(_).isEmpty)
        .map(d => refError(s"Invalid dimensionRef: '$d'", table, column))
      measureRefErrors = column.measureRefs
        .filter(schema.getMeasure(_).isEmpty)
        .map(m => refError(s"invalid measureRef: '$m'", table, column))
      tableRefErrors = column.tableRefs
        .filter(schema.getJoinDetails(_).isEmpty)
        .map(t => refError(s"Invalid tableRef: '$t'", table, column))
    } yield dimensionRefErrors ++ measureRefErrors ++ tableRefErrors).flatten

    val schemaNoExcludes = schema.copy(excludes = Set.empty)

    val defaultFilterErrors: Option[String] =
      schema.defaultFilter.flatMap { f =>
        val refs = f.collect {
          case UnresolvedField(name) if schemaNoExcludes.getField(name).isEmpty => name
        }
        refs.nonEmpty.option(
          s"default filter contains missing refs ${refs.mkString("[", ",", "]")}"
        )
      }

    val measureFilterErrors: List[String] = flatCollect(schema.measures) {
      case m: FilteredMeasure =>
        val (measureRefs, notFound) = m.filter
          .collect {
            case UnresolvedField(name) =>
              schemaNoExcludes.getField(name) match {
                case Some(_: Attribute) =>
                  None
                case Some(_: Measure) =>
                  Some(Left(name))
                case None =>
                  Some(Right(name))
              }
          }
          .flatten[Either[String, String]]
          .separate

        measureRefs.nonEmpty.option(
          s"filtered measure '${m.name}' references another measures ${measureRefs.mkString("[", ",", "]")} in filter"
        ) ++
          notFound.nonEmpty.option(
            s"measure '${m.name}' filter references ${notFound.mkString("[", ",", "]")} not found"
          )
    }

    /*
     * not checking this if measureFilterErrors are present
     * this method calls 'getAggregateCompatibility' method on schema which asserts
     * that all measure filter references are of compatible types (as it has to be checked during compilation)
     */
    val incompatibleOrUnreferencedFieldsErrors: List[String] =
      if (measureFilterErrors.nonEmpty) Nil
      else {

        val notReferenced = "is not referenced from any table"

        def chkMeasureReferenced(name: String) =
          schema.tables.forall(!schema.tableContainsMeasure(_, name)).option(s"measure '$name' $notReferenced")

        def chkAttrReferenced(name: String) =
          schema.tables.forall(!schema.tableContainsAttribute(_, name)).option(s"attribute '$name' $notReferenced")

        def chkCompatible(name: String, flds: Set[String]): Option[String] =
          getFields(schemaNoExcludes.getAggregateCompatibility(flds)).isEmpty
            .option(s"measure '$name' refers to the incompatible set of fields ${flds.mkString("[", ",", "]")}")

        val measureErrors: List[String] =
          schema.measures.collect {
            case m: AggMeasure => chkMeasureReferenced(m.name)
            // m.name == m.base.name is the case when filtered measure is defined without measure
            case m: FilteredMeasure =>
              (if (m.name == m.base.name) chkMeasureReferenced(m.name) else None)
                .orElse(chkCompatible(m.name, (m.base.name :: m.filter.collect { case fr: FieldRef => fr.name }).toSet))

            // Discuss : dont need to check CalculatedMeasure expression because it can contain only measures, and measures are by definition compatible with each other in the absense of dimensions
            // case m: CalculatedMeasure => chkCompatible(m.name, collectMeasures(m.calculation).map(_.merge.name))
          }.flatten

        val attributeRefErrors: List[String] = for {
          dim <- schema.dimensions
          hierarchy <- dim.hierarchies
          level <- hierarchy.levels
          attr <- level.attributes
          attrName = mkName(dim.name, hierarchy.name, level.name, attr.name)
          refErr <- chkAttrReferenced(attrName)
        } yield refErr

        measureErrors ++ attributeRefErrors
      }

    brokenTableRefErrs ++
      brokenDimensionColumns ++
      brokenMeasureColumns ++
      duplicateDimMesErrors ++
      duplicateTableErrors ++
      columnDuplicateErrors ++
      defaultFilterErrors ++
      measureFilterErrors ++
      incompatibleOrUnreferencedFieldsErrors
  }

  /**
    * Prepends all direct and transitive references in schema with its alias.
    * This method is used before merging schemas in case of aliased (non-wildcard) import (e.g.: import Foo or import Foo => Bar )
    *
    * @param schema  Target schema
    * @param alias  Schema alias
    * @return New schema - with all refs modified
    */
  private def withAlias(alias: String, schema: Schema): Schema = {

    def withAlias(s: String): String = alias + "." + s

    def aliasTable(table: Table): Table = {
      val newColumns = for {
        c <- table.columns
        newDimensionRefs = c.dimensionRefs.map(withAlias)
        newMeasureRefs = c.measureRefs.map(withAlias)
        newTableRefs = c.tableRefs.map(tr => tr.copy(tableName = withAlias(tr.tableName)))
      } yield c.copy(dimensionRefs = newDimensionRefs, measureRefs = newMeasureRefs, tableRefs = newTableRefs)

      table.copy(name = withAlias(table.name), columns = newColumns)
    }

    def aliasDimension(dimension: Dimension): Dimension = {
      val newHierarchies = dimension.hierarchies.map(
        h =>
          h.copy(
            table = h.table.map(withAlias),
            levels = h.levels.map(l =>
              l.copy(
                table = l.table.map(withAlias),
                columns = l.columns.map(c => if (c.contains(".")) withAlias(c) else c),
                conforms = l.conforms.map(withAlias),
                attributes = l.attributes.map(a =>
                  a.copy(table = a.table.map(withAlias),
                         columns = a.columns.map(c => if (c.contains(".")) withAlias(c) else c),
                         conforms = a.conforms.map(withAlias)))
            ))
        ))
      dimension.copy(name = withAlias(dimension.name),
                     table = dimension.table.map(withAlias),
                     conforms = dimension.conforms.map(withAlias),
                     hierarchies = newHierarchies)
    }

    def aliasFilter(p: Predicate) =
      p.transformUp { case UnresolvedField(f) => UnresolvedField(withAlias(f)) }.asInstanceOf[Predicate]

    def aliasAggMeasure(a: AggMeasure) =
      a.copy(
        name = withAlias(a.name),
        columns = a.columns.map(c => if (c.contains(".")) withAlias(c) else c)
      )

    def aliasFilteredMeasure(a: FilteredMeasure) =
      a.copy(name = withAlias(a.name), aliasAggMeasure(a.base), filter = aliasFilter(a.filter))

    def aliasNumericExpression(n: NumericExpression): NumericExpression = n match {
      case m: AggMeasure        => aliasAggMeasure(m)
      case m: FilteredMeasure   => aliasFilteredMeasure(m)
      case b: NumericBinaryExpr => b._copy(aliasNumericExpression(b.left), aliasNumericExpression(b.right))
      case l: Literal           => l
    }

    Schema(
      schema.name,
      schema.strict,
      schema.dimensions.map(aliasDimension),
      schema.measures.map {
        case m: AggMeasure      => aliasAggMeasure(m)
        case m: FilteredMeasure => aliasFilteredMeasure(m)
        case m: CalculatedMeasure =>
          m.copy(name = withAlias(m.name), calculation = aliasNumericExpression(m.calculation))
      },
      schema.tables.map(aliasTable),
      schema.defaultFilter.map(aliasFilter),
      schema.excludes.map(withAlias)
    )
  }

  private def processConformingDim(d: Dimension, to: Dimension, keepExistingOnNone: Boolean) = {
    def process[T](keep: Boolean, t: T)(o: Option[T]) = if (keep) o.orElse(Some(t)) else o
    def transformHierarchy(h: Hierarchy, to: Dimension, keepExistingOnNone: Boolean): Option[Hierarchy] =
      process(keepExistingOnNone, h)(
        to.hierarchies
          .find(_.name == h.name)
          .map(nextH => h.copy(levels = h.levels.flatMap(transformLevel(_, nextH, to, keepExistingOnNone))))
      )

    def transformLevel(l: Level, h: Hierarchy, d: Dimension, keepExistingOnNone: Boolean): Option[Level] =
      process(keepExistingOnNone, l)(
        h.levels
          .find(_.name == l.name)
          .map(nextL =>
            l.copy(attributes = l.attributes.flatMap(transformAttribute(_, nextL, h, d, keepExistingOnNone))))
      )

    def transformAttribute(a: Attribute,
                           l: Level,
                           h: Hierarchy,
                           dim: Dimension,
                           keepExistingOnNone: Boolean): Option[Attribute] =
      process(keepExistingOnNone, a)(
        l.attributes
          .find(_.name == a.name)
          .map { nextAttr =>
            val conformName = mkName(dim.name, h.name, l.name, nextAttr.name)
            if (a.conforms.contains(conformName)) a
            else a.copy(conforms = conformName :: a.conforms)
          }
      )

    d.copy(hierarchies = d.hierarchies.flatMap(transformHierarchy(_, to, keepExistingOnNone)))
  }

  private def inlineConformingDimension(dimension: Dimension, toInline: Dimension): Dimension =
    // populate only conforms for existing attributes
    processConformingDim(dimension, toInline, keepExistingOnNone = true)

  private def intersectConformingDimension(dimension: Dimension, toIntersect: Dimension): Dimension =
    if (dimension.hierarchies.nonEmpty) processConformingDim(dimension, toIntersect, keepExistingOnNone = false)
    else {
      // copy dimension from conforming one but without table and other physical attributes
      dimension.copy(
        conforms = Nil,
        hierarchies = toIntersect.hierarchies.map(
          h =>
            Hierarchy(
              h.name,
              levels = h.levels.map(l =>
                Level(l.name,
                      attributes = l.attributes.map(a =>
                        Attribute(a.name, conforms = List(mkName(toIntersect.name, h.name, l.name, a.name))))))
          ))
      )
    }

  /**
    * Populates hierarchies inside conforming dimensions down to attributes
    *
    * @return resulting schema when conforming dimensions are populated down to attributes
    */
  @tailrec
  private final def processConformingDimensions(schema: Schema): Either[String, Schema] = {

    val unmatchedDims = schema.dimensions.flatMap(_.conforms).toSet -- schema.dimensions.map(_.name)
    if (unmatchedDims.nonEmpty) Left(s"Conforming dimensions $unmatchedDims was not found")
    else {
      def _processConformingDimensions(schema: Schema, dimension: Dimension): Dimension = {
        val conformingDimensions = dimension.conforms.map(schema.getDimension(_).get)
        if (dimension.hierarchies.isEmpty) {
          conformingDimensions.foldLeft(dimension)((dim, cdim) => intersectConformingDimension(dim, cdim))
        } else {
          conformingDimensions.foldLeft(dimension)((dim, cdim) => inlineConformingDimension(dim, cdim))
        }
      }

      schema.dimensions.find(
        d =>
          d.conforms.nonEmpty &&
            d.conforms.forall(cname => schema.getDimension(cname).get.conforms.isEmpty)) match {
        case None => Right(schema)
        case Some(dimension) =>
          val newDim = _processConformingDimensions(schema, dimension).copy(conforms = Nil)
          val updatedSchema =
            schema.copy(dimensions = schema.dimensions.map(d => if (d.name == newDim.name) newDim else d))
          processConformingDimensions(updatedSchema)
      }
    }
  }

  private def addDefaultAttributes(dimensions: List[Dimension]): List[Dimension] = {
    // level semantically extends attribute, need to make sure that each level has an attribute with empty name to be able to query for levels.
    def levelWithDefaultAttribute(l: Level): Level =
      if (l.getAttribute(Array.empty).nonEmpty || l.name.isEmpty) l
      else
        l.copy(
          attributes = Attribute("", l.table, l.columns, l.expression, l.castType, l.metadata, l.conforms) :: l.attributes)

    dimensions.map(d =>
      d.copy(hierarchies = d.hierarchies.map(h => h.copy(levels = h.levels.map(levelWithDefaultAttribute)))))
  }

  private def compileMeasures(measureAsts: List[MeasureAST],
                              importedMeasures: Seq[Measure]): Either[NonEmptyList[String], List[Measure]] = {
    // capturing ref to deduplicate later. There is no need to show multiple errors caused by the same measure ref.
    case class RefError(ref: String, error: String)
    def notExist(ref: String): RefError = RefError(ref, s"Measure $ref does not exist")
    def typeIsNotCompatible(ref: String, ctx: String, allowedTypes: String*) = RefError(
      ref,
      s"Reference '$ref' is of incompatible type " +
        s"(only ${allowedTypes.mkString(" and ")} measures are allowed in the context of $ctx)"
    )

    val aggMeasuresAsts = measureAsts.collect { case m: AggMeasureAST           => m }
    val filteredMeasuresAsts = measureAsts.collect { case m: FilteredMeasureAST => m }
    val calcMeasureAsts = measureAsts.collect { case m: CalculatedMeasureAST    => m }

    // root measures are built from measure ASTs that do not depend on other measures (currently aggMeasuresAsts)
    val localRootMeasures: List[Measure] = aggMeasuresAsts.map(m =>
      m.filter match {
        case None => AggMeasure(m.name, m.aggregate, m.columns, m.metadata, m.conforms)
        case Some(filter) =>
          FilteredMeasure(m.name, AggMeasure(m.name, m.aggregate, m.columns, m.metadata, m.conforms), filter)
    })(collection.breakOut)

    val commonDependencies = localRootMeasures ++ importedMeasures

    val filteredMeasures: Either[NonEmptyList[RefError], List[FilteredMeasure]] = {
      filteredMeasuresAsts.traverse { m =>
        def typeNotCompatible = typeIsNotCompatible(m.ref, s"FilteredMeasure '${m.name}'", "Aggregated")
        (for {
          // related idea bug: https://youtrack.jetbrains.com/issue/SCL-10259
          refMeasure <- commonDependencies
            .find(_.name == m.ref)
            .toRight(
              // calcMeasureAsts and filteredMeasuresAsts are not on common dependencies
              if (filteredMeasuresAsts.exists(_.name == m.ref) || calcMeasureAsts.exists(_.name == m.ref))
                typeNotCompatible
              else notExist(m.ref)
            )
          validRefMeasure <- refMeasure match {
            case v: AggMeasure                             => Right(v)
            case _: CalculatedMeasure | _: FilteredMeasure => Left(typeNotCompatible)
          }
        } yield FilteredMeasure(m.name, validRefMeasure, m.filter, m.metadata)).toValidatedNel
      }.toEither
    }

    def calcMeasures(deps: Seq[Measure]): Either[NonEmptyList[RefError], List[CalculatedMeasure]] = {

      def compileExpressions(
          _ast: NumericExpressionAST,
          deps: Seq[Measure],
          calcMeasureName: String
      ): Either[RefError, NumericExpression] = {

        def _compileExpressions(ast: NumericExpressionAST): Either[RefError, NumericExpression] = {
          ast match {
            // idea bug: https://youtrack.jetbrains.com/issue/SCL-12892
            case PlusAST(l, r)     => _compileExpressions(l).map2(_compileExpressions(r))(Plus)
            case MinusAST(l, r)    => _compileExpressions(l).map2(_compileExpressions(r))(Minus)
            case DivideAST(l, r)   => _compileExpressions(l).map2(_compileExpressions(r))(Divide)
            case MultiplyAST(l, r) => _compileExpressions(l).map2(_compileExpressions(r))(Multiply)
            case n: NumericLiteral => Right(n)
            case UnresolvedField(ref) =>
              def typeNotCompatible = typeIsNotCompatible(
                ref,
                s"NumericExpression in Calculated measure '$calcMeasureName'",
                "Aggregated",
                "Filtered"
              )
              for {
                refMeasure <- deps
                  .find(_.name == ref)
                  .toRight(if (calcMeasureAsts.exists(_.name == ref)) typeNotCompatible else notExist(ref))
                r <- refMeasure match {
                  case n: AggMeasure      => Right(n)
                  case n: FilteredMeasure => Right(n)
                  case _                  => Left(typeNotCompatible)
                }
              } yield r
          }
        }
        _compileExpressions(_ast)
      }

      calcMeasureAsts
        .traverse(
          m =>
            compileExpressions(m.calculation, deps, m.name).toValidatedNel
              .map(CalculatedMeasure(m.name, _, m.metadata))
        )
        .toEither
    }

    (for {
      fm <- filteredMeasures
      cm <- calcMeasures(commonDependencies ::: fm)
    } yield localRootMeasures ::: fm ::: cm)
    // deduplicating errors by causing measure ref
      .leftMap(errors => NonEmptyList.fromListUnsafe(distinctBy(errors.toList)(_.ref).map(_.error)))
  }

  private def compileTables(
      table: TableAST,
      schemaDataSource: Option[DataSource],
      dataSources: Seq[DataSource]
  ): Either[String, Table] =
    for {
      dsOpt <- table.dataSource.traverse(n => dataSources.find(_.name == n).toRight(s"Data source $n not found"))
      ds <- dsOpt
        .orElse(schemaDataSource)
        .toRight(
          s"Data source is not defined on table ${table.name} and no global data source is defined on schema level)")
    } yield
      Table(
        table.name,
        table.definition.getOrElse(Left(PhysicalTableName(table.name))),
        ds,
        table.columns,
        table.wildcardColumns.nonEmpty
      )

  type TableName = String
  private def inferColumnsFromRefs(tables: List[Table],
                                   dimensions: List[Dimension],
                                   measures: List[Measure]): Map[TableName, List[Column]] = {

    val dimensionAttributesDerivedColumns: List[(String, Column)] =
      for {
        dim <- dimensions
        hierarchy <- dim.hierarchies
        level <- hierarchy.levels
        attr <- level.attributes
        column <- if (attr.columns.nonEmpty) attr.columns
        else if (attr.name.nonEmpty) List(attr.name)
        else List(level.name)
        (table, columnName) = readColumnRef(column)
        tableName <- table.orElse(attr.table).orElse(level.table).orElse(hierarchy.table).orElse(dim.table)
      } yield {
        val dimRef = mkName(dim.name, hierarchy.name, level.name, attr.name)
        (tableName, Column(columnName, dimensionRefs = Set(dimRef)))
      }

    val measuresDerivedColumns: List[(String, Column)] =
      for {
        measure <- measures.collect {
          case m: AggMeasure      => m
          case f: FilteredMeasure => f.base
        }
        column <- measure.columns
        (table, columnName) = readColumnRef(column)
        tableName <- table
      } yield (tableName, Column(columnName, measureRefs = Set(measure.name)))

    val tableRefDerivedColumns: List[(String, Column)] =
      tables
        .flatMap(_.columns)
        .flatMap(_.tableRefs)
        .map(tr => tr.tableName -> Column(tr.colName))

    (dimensionAttributesDerivedColumns
      ::: measuresDerivedColumns
      ::: tableRefDerivedColumns).groupBy(_._1).mapValues(_.map(_._2))
  }

  /**
    * Add tables referenced from dimensions and other tables
    * @param dimensions
    * @param existingTables
    * @param schemaDataSource
    * @return the list of new tables
    */
  private def getAdditionalTables(
      dimensions: List[Dimension],
      existingTables: List[Table],
      schemaDataSource: Option[DataSource]
  ): Either[String, List[Table]] = {

    val existingTableNames: Set[String] = existingTables.map(_.name)(collection.breakOut)

    val dimensionReferencedTables: Set[String] = dimensions.flatMap(
      d =>
        d.table ++
          d.hierarchies.flatMap(
            h =>
              h.table ++
                h.levels.flatMap(l =>
                  l.table ++
                    l.attributes.flatMap(_.table))))(collection.breakOut)

    val tableReferencedTables = existingTables.flatMap(_.columns.flatMap(_.tableRefs.map(tr => tr.tableName)))

    val newTableNames = (dimensionReferencedTables ++ tableReferencedTables) -- existingTableNames

    if (newTableNames.isEmpty) Right(Nil)
    else {
      schemaDataSource
        .map[List[Table]](ds =>
          newTableNames.map(name => Table(name, Left(PhysicalTableName(name)), ds, Nil, false))(collection.breakOut))
        .toRight(
          s"Table references ${newTableNames.mkString("[", ",", "]")} are not found in tables " +
            s"and no datasource is defined on Schema level to create new ones"
        )
    }
  }

  private def addColumns(table: Table, inferredColumnsFromRefs: Map[TableName, List[Column]]): Table =
    inferredColumnsFromRefs.get(table.name).fold(table) { inferredColumns =>
      val distinctInferredNames: Set[String] = inferredColumns.map(_.name)(collection.breakOut)
      table.copy(
        columns = table.columns ++ (distinctInferredNames -- table.columns.map(_.name)).map(Column(_))
      )
    }

  // enriching existing and inferred tables with refs to other tables and dimensions
  private def enrichWithRefs(allTables: List[Table],
                             inferredColumnsFromRefs: Map[TableName, List[Column]]): List[Table] = {

    def mergeColumns(target: Column, changes: Column): Column =
      target.copy(
        dimensionRefs = target.dimensionRefs ++ changes.dimensionRefs,
        measureRefs = target.measureRefs ++ changes.measureRefs,
        tableRefs = target.tableRefs ++ changes.tableRefs
      )

    allTables.map(
      tbl =>
        inferredColumnsFromRefs
          .get(tbl.name)
          .fold(tbl)(inferredCols =>
            tbl.copy(
              columns = joinUsing(tbl.columns, inferredCols)(_.name) { (col, inferred) =>
                inferred.foldLeft(col)(mergeColumns)
              }
          )))
  }
}
