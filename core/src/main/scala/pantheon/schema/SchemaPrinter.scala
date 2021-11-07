package pantheon.schema

import pantheon.planner._
import cats.syntax.either._
import pantheon.util.BoolOps

// Warning: Predicate serializer depends on this logic
object SchemaPrinter {
  val ident = "  "
  val initalIdentLevel = 0
  val identIncr = 1

  def next(ident: Int): Int = ident + identIncr

  def print(schema: SchemaAST): String = {
    "schema " + printName(schema.name) +
      printParams("dataSource" -> schema.dataSource) +
      printBody(initalIdentLevel)(
        schema.imports.map(printImportExport("import ")) ++
          schema.exports.map(printImportExport("export ")),
        schema.dimensions.map(printDimension(next(initalIdentLevel))),
        schema.measures.map(printMeasure(next(initalIdentLevel))),
        schema.tables.map(printTable(next(initalIdentLevel))),
        printDefaultFilter(next(initalIdentLevel))(schema.defaultFilter) :: Nil
      )
  }

  private def printBody(identLevel: Int)(bodyItemGroups: Iterable[String]*): String = {
    val bi = bodyItemGroups.map(_.filterNot(_.isEmpty)).filterNot(_.isEmpty)
    if (bi.isEmpty) ""
    else bi.map(_.mkString("\n")).mkString(" {\n", "\n\n", s"\n${ident * identLevel}}")
  }

  private def printParams(params: (String, Iterable[String])*): String = {
    def printParam(name: String, value: Iterable[String]): String =
      if (value.size > 1) name + "s = " + value.map(printStringLiteral).mkString("[", ", ", "]")
      else name + " = " + printStringLiteral(value.head)

    if (params.forall(_._2.isEmpty)) ""
    else {
      " (" + params
        .withFilter(_._2.nonEmpty)
        .map {
          case (name, value) =>
            printParam(name, value)
        }
        .mkString(", ") + ")"
    }
  }

  private def printStringLiteral(l: String): String = {
    val q3 = "\"\"\""
    if ("\"\\\n\t\f\r".exists(l.contains(_))) q3 + l + q3
    else
      s"""\"$l\""""
  }

  private def printName(name: String): String = {
    name
  }

  private def printColumnName(name: String): String = {
    if (name.nonEmpty && (!Character.isJavaIdentifierStart(name.head) || !name.forall(Character.isJavaIdentifierPart)))
      "\"" + name + "\""
    else name
  }

  def printImportExport(prefix: String)(_import: AliasedName): String =
    ident + prefix + (_import.alias match {
      case None        => s"${_import.name}._"
      case Some(alias) => if (alias == _import.name) alias else s"{${_import.name} => $alias}"
    })

  private def printDimension(identLevel: Int)(dim: Dimension): String = {
    ident + "dimension " + printName(dim.name) +
      printParams("table" -> dim.table) +
      printBody(identLevel)(
        List(
          printMetadata(next(identLevel))(dim.metadata),
          printConforms(next(identLevel))(dim.conforms)
        ),
        dim.hierarchies.map(printHierarchy(next(identLevel)))
      )
  }

  def printMetadata(identLevel: Int)(metadata: ValueMap): String =
    if (metadata.isEmpty) ""
    else {
      ident * identLevel + "metadata " +
        metadata.map(md => s"${md._1} = " + printMetadataValue(md._2)).mkString("(", ", ", ")")
    }

  def printMetadataValue(v: Value[_]): String = v match {
    case BooleanValue(b) => b.toString
    case StringValue(s)  => printStringLiteral(s)
    case NumericValue(n) => n.toString()
    case ArrayValue(a)   => a.map(printMetadataValue).mkString("[", ", ", "]")
  }

  def printConforms(identLevel: Int)(conforms: List[String]): String =
    if (conforms.isEmpty) ""
    else ident * identLevel + "conforms " + conforms.mkString(", ")

  def printHierarchy(identLevel: Int)(h: Hierarchy): String = {
    ident * identLevel + "hierarchy " + printName(h.name) +
      printParams("table" -> h.table) +
      printBody(identLevel)(
        List(printMetadata(next(identLevel))(h.metadata)),
        h.levels.map(printLevel(next(identLevel)))
      )
  }

  def printLevel(identLevel: Int)(l: Level): String = {
    ident * identLevel + "level " + printName(l.name) +
      printParams(
        "table" -> l.table,
        "column" -> l.columns,
        "expression" -> l.expression,
        "castType" -> l.castType
      ) +
      printBody(identLevel)(
        List(
          printMetadata(next(identLevel))(l.metadata),
          printConforms(next(identLevel))(l.conforms)
        ),
        l.attributes.map(printAttribute(next(identLevel)))
      )
  }

  def printAttribute(identLevel: Int)(a: Attribute): String = {
    ident * identLevel + "attribute " + printName(a.name) +
      printParams(
        "table" -> a.table,
        "column" -> a.columns,
        "expression" -> a.expression,
        "castType" -> a.castType
      ) +
      printBody(identLevel)(
        List(
          printMetadata(next(identLevel))(a.metadata),
          printConforms(next(identLevel))(a.conforms)
        )
      )

  }

  def printMeasure(identLevel: Int)(m: MeasureAST): String = {

    val commonBodyParts: List[String] = List(printMetadata(next(identLevel))(m.metadata))

    val (params, bodyParts) = m match {
      case m: CalculatedMeasureAST =>
        val params = List("calculation" -> Seq(printNumericExpression(m.calculation)))
        params -> Nil

      case m: FilteredMeasureAST =>
        val params = List(("ref" -> Seq(m.ref)), "filter" -> Seq(printExpression(m.filter)))
        params -> Nil

      case m: AggMeasureAST =>
        val params = ("aggregate" -> Seq(m.aggregate.toString)) ::
          ("column" -> m.columns) ::
          m.filter.map(f => "filter" -> Seq(printExpression(f))).toList
        params -> List(printConforms(next(identLevel))(m.conforms))
    }

    ident * identLevel + "measure " + printName(m.name) +
      printParams(params: _*) +
      printBody(identLevel)(commonBodyParts ::: bodyParts)
  }

  def printTable(identLevel: Int)(t: TableAST): String = {
    ident * identLevel + "table " + printName(t.name) +
      printParams(
        "dataSource" -> t.dataSource,
        "physicalTable" -> t.definition.flatMap(_.left.toOption.map(_.value)),
        "sql" -> t.definition.flatMap(_.toOption.map(_.value))
      ) +
      printBody(identLevel)(
        t.wildcardColumns.map(_ => ident * next(identLevel) + "columns *") ++
          t.columns.map(printColumn(next(identLevel)))
      )

  }

  def printTableRef(tableRef: TableRef): String = {
    val join = if (tableRef.joinType != JoinType.Left) s" ${tableRef.joinType.toString.toLowerCase}" else ""
    s"${tableRef.tableName}.${tableRef.colName}$join"
  }

  def printColumn(identLevel: Int)(c: Column): String = {
    ident * identLevel + "column " + printColumnName(c.name) +
      printParams(
        "expression" -> c.expression,
        "tableRef" -> c.tableRefs.map(printTableRef)
      )
  }

  def printDefaultFilter(identLevel: Int)(defaultFilter: Option[Predicate]): String = {
    if (defaultFilter.isEmpty) ""
    else
      ident * identLevel + "filter \"" + defaultFilter.map(printExpression).get + "\""
  }

  def printExpression(expr: Expression): String = {
    def print(expr: Expression, addParen: Boolean = false): String = expr match {
      case EqualTo(left, right)            => print(left) + " = " + print(right)
      case IsNull(e)                       => s"${print(e)} is null"
      case IsNotNull(e)                    => s"${print(e)} is not null"
      case NotEqual(left, right)           => print(left) + " != " + print(right)
      case GreaterThan(left, right)        => print(left) + " > " + print(right)
      case LessThan(left, right)           => print(left) + " < " + print(right)
      case GreaterThanOrEqual(left, right) => print(left) + " >= " + print(right)
      case LessThanOrEqual(left, right)    => print(left) + " <= " + print(right)
      case In(left, right)                 => print(left) + " in " + right.map(print(_)).mkString("(", ", ", ")")
      case NotIn(left, right)              => print(left) + " not in " + right.map(print(_)).mkString("(", ", ", ")")
      case Like(left, right)               => print(left) + " like " + right.map(print(_)).mkString("(", ", ", ")")
      case And(left, right)                => print(left, true) + " and " + print(right, true)
      case Or(left, right) => {
        val (open, close) = if (addParen) "(" -> ")" else "" -> ""
        open + print(left) + " or " + print(right) + close
      }
      case BindVariable(name)    => ":" + name
      case UnresolvedField(name) => name
      case l: literal.Literal    => printLiteral(l)
    }

    print(expr)
  }

  object Brackets extends Enumeration {
    val Left, Right, None = Value
  }

  def printNumericExpression(expr: NumericExpressionAST, brackets: Brackets.Value = Brackets.None): String = {
    def addBrackets(s: String, hightPriority: Boolean) =
      if (brackets == Brackets.None || (brackets == Brackets.Left && hightPriority)) s
      else s"($s)"

    expr match {
      case PlusAST(left, right) =>
        addBrackets(
          printNumericExpression(left, Brackets.None) + " + " + printNumericExpression(right, Brackets.None),
          false
        )
      case MinusAST(left, right) =>
        addBrackets(
          printNumericExpression(left, Brackets.None) + " - " + printNumericExpression(right, Brackets.None),
          false
        )
      case MultiplyAST(left, right) =>
        addBrackets(
          printNumericExpression(left, Brackets.Left) + " * " + printNumericExpression(right, Brackets.Right),
          true
        )
      case DivideAST(left, right) =>
        addBrackets(
          printNumericExpression(left, Brackets.Left) + " / " + printNumericExpression(right, Brackets.Right),
          true
        )
      case l: literal.Literal    => printLiteral(l)
      case UnresolvedField(name) => name
    }
  }

  def printLiteral(lit: literal.Literal): String = {
    import literal._
    lit match {
      case Timestamp(v) => s"timestamp '${v.toString}'"
      case Date(v)      => s"date '${v.toString}'"
      case Boolean(v)   => v.toString
      case Integer(v)   => v.toString
      case Long(v)      => v.toString
      case Double(v)    => v.toString
      case Decimal(v)   => v.toString
      case String(v)    => s"'$v'"
      case Null         => "null"
    }
  }
}
