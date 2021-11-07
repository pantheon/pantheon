package pantheon.schema.parser

import pantheon.schema._
import pantheon.schema.parser.grammar.PslParser._
import cats.instances.list._
import cats.instances.either._
import cats.syntax.either._
import cats.instances.option._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.syntax.apply._

import reflect.internal.util.Collections.distinctBy
import ParserUtils._
import pantheon.planner.Predicate
import pantheon.schema.{Column, SchemaAST, TableAST, TableRef}

import scala.collection.JavaConverters._
import pantheon.schema.parser.grammar.{PslLexer, PslParser}
import pantheon.util.Tap

object ErrListener extends ExceptionThrowingErrorListener

object SchemaParser {

  def apply(psl: String): Either[String, SchemaAST] =
    parse(new PslParser(_), new PslLexer(_))(psl, parser => readSchema(parser.schema())).joinRight

  private def parseFilter(f: String) =
    FilterParser(f).left.map(s"failed to parse filter '$f'. Reason: " + _)
  private def parseNumericExpr(e: String) =
    NumExprParser(e).left.map(s"failed to parse numeric expression '$e'. Reason: " + _)

  private def readConforms(c: java.util.List[PslParser.ConformsContext]): List[String] =
    c.asScala.map(_.identWithDot().getText).toList

  private def readMetaData(metaData: MetadataContext): Option[ValueMap] = {

    def readMetaDataItem(item: MetadataItemContext): (String, Value[_]) = {
      val name = item.metaDataItemName().getText
      val value: Value[_] = {

        def readPrimitive(pv: PrimitiveValueContext): PrimitiveValue[_] = {

          oneOf(s"Primitive value")(
            pv.BOOL().?|>(v => BooleanValue(v.getText.toBoolean)),
            pv.STRINGLITERAL().?|>(v => StringValue(v.unquote)),
            pv.INTEGER().?|>(v => NumericValue(v.getText.toInt)),
            pv.DECIMAL().?|>(v => NumericValue(BigDecimal(v.getText)))
          )
        }

        oneOf(s"Metadata item")(
          item.primitiveValue() ?|> readPrimitive,
          item.arrayValue().?|>(v => ArrayValue(v.primitiveValue().asScala.map(readPrimitive).toList))
        )

      }
      name -> value
    }

    metaData.?|>>[ValueMap](
      _.metadataItems().?|>(_.metadataItem().asScala.map(readMetaDataItem)(collection.breakOut))
    )
  }

  private def readDimension(ctx: DimensionContext): Either[String, Dimension] = {

    case class HierarchyLevelParams(
        table: Option[String],
        columns: List[String],
        expression: Option[String],
        castType: Option[String]
    )

    def readHierarchies(hierarchiesCtx: DimensionHierarchiesContext): Either[String, List[Hierarchy]] = {

      def readHierarchy(h: HierarchyContext): Either[String, Hierarchy] = {
        val name = h.identifier().getText

        for {
          params <- readStringParams(s"Hierarchy $name", readSeq(h.hierarchyParams())(_.hierarchyParam))(
            _.tableP --> (_.STRINGLITERAL())
          )
          table :: Nil = params
          bodyOpt = Option(h.hierarchyBody())
          levels <- bodyOpt.toList.flatMap(_.hierarchyLevel().asScala.toList).traverse(readLevel)
          metadata = bodyOpt.flatMap(b => readMetaData(b.metadata())).getOrElse(Map.empty)
        } yield {
          Hierarchy(
            name = name,
            table = table,
            metadata = metadata,
            levels = levels
          )
        }
      }

      def readLevel(lvl: HierarchyLevelContext): Either[String, Level] = {
        val name = lvl.identifier().getText
        for {
          ps <- readLevelParams(s"Level $name", lvl.hierarchyLevelParams())
          bodyOpt = Option(lvl.hierarchyLevelBody())
          attrs <- bodyOpt.toList.flatMap(_.hierarchyLevelAttribute().asScala).traverse(readAttribute)
        } yield {
          Level(
            name,
            ps.table,
            ps.columns,
            ps.expression,
            ps.castType,
            bodyOpt.flatMap(b => readMetaData(b.metadata())).getOrElse(Map.empty),
            bodyOpt.map(b => readConforms(b.conforms())).getOrElse(Nil),
            attrs
          )
        }
      }

      oneOf(s"Dimension hierarchies: ${hierarchiesCtx.getText}")(
        readNel(hierarchiesCtx.hierarchy()).map(_.traverse(readHierarchy)),
        readNel(hierarchiesCtx.hierarchyLevel()).map(_.traverse(readLevel).map(v => List(Hierarchy("", levels = v)))),
        readNel(hierarchiesCtx.hierarchyLevelAttribute()).map(
          _.traverse(readAttribute(_)).map(_attrs => List(Hierarchy("", levels = List(Level("", attributes = _attrs)))))
        )
      )
    }

    def readLevelParams(lbl: String, p: HierarchyLevelParamsContext): Either[String, HierarchyLevelParams] = {
      for {
        params <- readParams(lbl, readSeq(p)(_.hierarchyLevelParam()))(
          _.tableP().-->(v => List(v.STRINGLITERAL().unquote)),
          _.columnRefP().-->(v => List(v.STRINGLITERAL().unquote)),
          _.columnRefsP().-->(_.multiStringValue().STRINGLITERAL().asScala.map(_.unquote).toList),
          _.expressionP().-->(v => List(v.STRINGLITERAL().unquote)),
          _.castTypeP().-->(v => List(v.STRINGLITERAL().unquote))
        )
        table :: columnRef :: columnRefs :: expression :: castType :: Nil = params
        _ <- Either.cond(columnRef.isEmpty || columnRefs.isEmpty,
                         (),
                         "Either columnRef or columnRefs should be defined, not both")

      } yield
        HierarchyLevelParams(table.map(_.head),
                             columnRef.orElse(columnRefs).getOrElse(Nil),
                             expression.map(_.head),
                             castType.map(_.head))
    }

    def readAttribute(attr: HierarchyLevelAttributeContext): Either[String, Attribute] = {
      val name = attr.identifier().getText
      readLevelParams(s"HierarchyLevelAttribute $name", attr.hierarchyLevelParams()).map { ps =>
        val bodyOpt = Option(attr.hierarchyLevelAttributeBody())
        val metaData = bodyOpt.flatMap(v => readMetaData(v.metadata())).getOrElse(Map.empty)
        val conforms = bodyOpt.map(b => readConforms(b.conforms())).getOrElse(Nil)
        Attribute(name, ps.table, ps.columns, ps.expression, ps.castType, metaData, conforms)
      }
    }

    {
      val dName = ctx.identifier().getText
      val dParams = readSeq(ctx.dimensionParams())(_.dimensionParam())

      for {
        params <- readStringParams(s"Dimension $dName", dParams)(
          _.tableP().-->(_.STRINGLITERAL())
        )
        table :: Nil = params
        bodyOpt = Option(ctx.dimensionBody())
        hierarchies <- bodyOpt.flatMap(b => Option(b.dimensionHierarchies())).traverse(readHierarchies)
      } yield
        Dimension(
          name = dName,
          table = table,
          metadata = bodyOpt.flatMap(b => readMetaData(b.metadata())).getOrElse(Map.empty),
          conforms = bodyOpt.map(_.conforms().asScala.toList.map(_.identWithDot().getText)).getOrElse(Nil),
          hierarchies = hierarchies.getOrElse(Nil)
        )
    }

  }

  private def readMeasure(ctx: MeasureContext): Either[String, MeasureAST] = {

    case class MeasureDefinition(agg: Option[MeasureAggregate],
                                 measure: Option[String],
                                 filter: Option[Predicate],
                                 calculation: Option[String],
                                 columns: List[String],
                                 metadata: ValueMap,
                                 conforms: List[String])

    val mName = ctx.identifier().getText

    def paramIsNotAllowedMsg(param: String, measureType: String) = s"'$param' is not allowed in '$measureType' measure"
    def missingParamsMsg(param: String, measureType: String) = s"missing '$param' to define '$measureType' measure"

    def readAggMeasure(allFields: MeasureDefinition): Either[String, AggMeasureAST] = {
      import allFields._
      measure
        .as(paramIsNotAllowedMsg("measure", "Aggregate"))
        .toLeft(AggMeasureAST(mName, agg.getOrElse(MeasureAggregate.Sum), columns, filter, metadata, conforms))
    }

    def readFilteredMeasure(
        allFields: MeasureDefinition,
        filter: Predicate,
        measure: String
    ): Either[String, FilteredMeasureAST] =
      allFields.conforms.headOption
        .as(paramIsNotAllowedMsg("conforms", "Filtered"))
        .as(paramIsNotAllowedMsg("column(s)", "Filtered"))
        .toLeft(FilteredMeasureAST(mName, measure, filter, allFields.metadata))

    def readCalculatedMeasure(allFields: MeasureDefinition,
                              calculation: String): Either[String, CalculatedMeasureAST] = {
      val measureType = "Calculated"
      for {
        _ <- allFields.measure.as(paramIsNotAllowedMsg("measure", measureType)).toLeft(())
        _ <- allFields.conforms.headOption.as(paramIsNotAllowedMsg("conforms", measureType)).toLeft(())
        _ <- allFields.conforms.headOption.as(paramIsNotAllowedMsg("column(s)", measureType)).toLeft(())
        expr <- parseNumericExpr(calculation)
      } yield CalculatedMeasureAST(mName, expr, allFields.metadata)
    }

    for {
      params <- readParams(s"Measure $mName", readSeq(ctx.measureParams())(_.measureParam))(
        _.aggregateP().-->(v => List(v.STRINGLITERAL().unquote)),
        _.filterP().-->(v => List(v.STRINGLITERAL().unquote)),
        _.measureP().-->(v => List(v.STRINGLITERAL().unquote)),
        _.calculationP().-->(v => List(v.STRINGLITERAL().unquote)),
        _.columnRefP().-->(v => List(v.STRINGLITERAL().unquote)),
        _.columnRefsP().-->(_.multiStringValue().STRINGLITERAL().asScala.map(_.unquote).toList)
      )

      aggregate :: filter :: measure :: calculation :: columnRef :: columnRefs :: Nil = params

      _ <- Either.cond(columnRef.isEmpty || columnRefs.isEmpty,
                       (),
                       "Either columnRef or columnRefs should be defined, not both")

      agg <- aggregate
        .map(_.head)
        .traverse(agg => MeasureAggregate.withNameInsensitiveOption(agg).toRight(s"invalid aggregation '$agg'"))
      filter <- filter.map(_.head).traverse(parseFilter)
      bodyOpt = Option(ctx.measureBody())

      definition = MeasureDefinition(
        agg,
        measure.map(_.head),
        filter,
        calculation.map(_.head),
        columnRef.orElse(columnRefs).getOrElse(Nil),
        metadata = bodyOpt.flatMap(b => readMetaData(b.metadata())).getOrElse(Map.empty),
        conforms = bodyOpt.map(_.conforms().asScala.toList.map(_.identWithDot().getText)).getOrElse(Nil)
      )

      _ <- Either.cond(
        (definition.filter ++ definition.calculation ++ definition.agg).size <= 1,
        (),
        "Params ['filter', 'calculation', 'aggregate'] may not be combined in single measure definition"
      )

      measure <- (definition.filter, definition.measure)
        .mapN(readFilteredMeasure(definition, _, _))
        .orElse(definition.calculation.map(readCalculatedMeasure(definition, _)))
        .getOrElse(readAggMeasure(definition))

    } yield measure

  }

  private def readSchemaImportExport(ctx: ImportExportBodyContext): Seq[AliasedName] =
    oneOfOpt("schemaImport", ctx)(
      _.wildcardNameBlock().?|>(uaCtx => Seq(AliasedName(uaCtx.identifier().getText, None))),
      _.aliasedNamesBlock().?|>(_.aliasedName().asScala.map { an =>
        val name = an.identifier().getText
        AliasedName(name, an.alias().?|>(_.identifier().getText).orElse(Some(name)))
      })
    )

  private def readTableRef(ref: TableRefContext): Either[String, TableRef] = {
    val tableName = ref.tableRefName().identifier().getText
    val columnName =
      oneOf("Table ref column name", ref.tableRefName().quotedIdentifier())(
        _.identifier(),
        _.STRINGLITERAL()
      ).getText

    for {
      joinType <- Option(ref.tableRefParams()).flatTraverse(
        params =>
          params
            .joinTypeP()
            .?|>(_.STRINGLITERAL().unquote)
            .traverse(joinTypeStr =>
              Either
                .catchNonFatal(JoinType.withName(joinTypeStr.capitalize))
                .leftMap(e => s"Invalid table reference: ${ref.getText}, error parsing join type: $e")))
    } yield TableRef(tableName, columnName, joinType.getOrElse(JoinType.Left))
  }

  private def readTable(ctx: TableContext): Either[String, TableAST] = {
    val name = ctx.identifier().getText

    def readColumn(colCtx: TableColumnContext): Either[String, Column] = {

      val name =
        oneOf("Table column name", colCtx.quotedIdentifier())(
          _.identifier(),
          _.STRINGLITERAL()
        ).getText

      for {
        params <- readParams("Column", readSeq(colCtx.tableColumnParams())(_.tableColumnParam()))(
          _.expressionP() --> (v => List(v.STRINGLITERAL().unquote)),
          _.tableRefP() --> (v => List(v.STRINGLITERAL().unquote)),
          _.tableRefsP() --> (_.multiStringValue().STRINGLITERAL().asScala.map(_.unquote).toList)
        )
        expression :: tableRef :: tableRefs :: Nil = params
        _ <- Either.cond(tableRef.isEmpty || tableRefs.isEmpty,
                         (),
                         "Either tableRef or tableRefs should be defined, not both")
        tableRefsParams <- tableRef.orElse(tableRefs).getOrElse(Nil).traverse { ref =>
          val (tableName, _colName) = ref.splitAt(ref.lastIndexOf("."))
          val colName = _colName.stripPrefix(".")
          Either.cond(
            tableName.nonEmpty || colName.nonEmpty,
            TableRef(tableName, colName),
            s"Invalid table reference: $ref, tableName or colName is not defined"
          )
        }
        tableRefsBody <- readSeq(colCtx.tableColumnBody())(_.tableRef()).toList.traverse(readTableRef)
      } yield {
        Column(
          name = name,
          expression = expression.map(_.head),
          dimensionRefs = Set.empty,
          measureRefs = Set.empty,
          tableRefs = (tableRefsParams ++ tableRefsBody).toSet
        )
      }
    }

    for {
      params <- readStringParams(s"Table $name", readSeq(ctx.tableParams())(_.tableParam()))(
        _.physicalTableP() --> (_.STRINGLITERAL()),
        _.dataSourceP() --> (_.STRINGLITERAL()),
        _.sqlP() --> (_.STRINGLITERAL())
      )
      physicalTable :: dataSource :: sql :: Nil = params
      columns <- readSeq(ctx.tableBody())(_.tableColumn()).toList.traverse(readColumn)
      wildcardColumns: List[Unit] = readSeq(ctx.tableBody())(_.WILDCARDCOLUMN()).map(_ => ())(collection.breakOut)
      _ <- Either.cond(physicalTable.isEmpty || sql.isEmpty,
                       (),
                       s"Both physical table and sql is defined in table $name")
    } yield
      TableAST(
        name,
        physicalTable.map(v => Left(PhysicalTableName(v))).orElse(sql.map(v => Right(SqlExpression(v)))),
        dataSource,
        columns,
        wildcardColumns
      )
  }

  private def readSchema(ctx: PslParser.SchemaContext): Either[String, SchemaAST] = {
    val name = ctx.identifier().getText
    val bodyOpt = Option(ctx.schemaBody()).toList

    for {
      dimensions <- bodyOpt.flatMap(_.dimension.asScala).traverse(readDimension)
      params <- readParams(s"Schema $name", readSeq(ctx.schemaParams())(_.schemaParam()))(
        _.strictP() --> (_.BOOL().getText),
        _.dataSourceP() --> (_.STRINGLITERAL().unquote)
      )
      strict :: dataSource :: Nil = params
      measures <- bodyOpt.flatMap(_.measure.asScala).traverse(readMeasure)
      tables <- bodyOpt.flatMap(_.table().asScala).traverse(readTable)
      _ <- Either.cond(distinctBy(tables)(_.name).size == tables.size, (), "duplicate table names detected")
      importsOpt = bodyOpt.flatMap(_.schemaImports().asScala.flatMap(v => readSchemaImportExport(v.importExportBody())))
      exportsOpt = bodyOpt.flatMap(_.schemaExports().asScala.flatMap(v => readSchemaImportExport(v.importExportBody())))
      filters <- bodyOpt.flatMap(_.filter().asScala).traverse(f => parseFilter(f.STRINGLITERAL().unquote))
      filter <- Either.cond(filters.size <= 1, filters.headOption, s"More than one filter detected in schema: $filters")
    } yield
      SchemaAST(
        name,
        dataSource,
        strict.fold(true)(_.toBoolean),
        dimensions,
        measures,
        tables,
        importsOpt,
        exportsOpt,
        filter
      )
  }

}
