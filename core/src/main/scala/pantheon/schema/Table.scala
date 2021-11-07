package pantheon.schema

import pantheon.DataSource

// consider using type tags for cases like this
case class PhysicalTableName(value: String)
case class SqlExpression(value: String)

case class TableAST(name: String,
                    definition: Option[Either[PhysicalTableName, SqlExpression]] = None,
                    dataSource: Option[String] = None,
                    columns: List[Column] = List(),
                    wildcardColumns: List[Unit])

case class Table(name: String,
                 definition: Either[PhysicalTableName, SqlExpression],
                 dataSource: DataSource,
                 columns: List[Column] = Nil,
                 addAllDBColumns: Boolean = false) {

  // TODO: change this
  override def equals(obj: scala.Any) = super[Object].equals(obj)

  def getColumn(name: String): Option[Column] = columns.find(_.name == name)
}

object JoinType extends Enumeration {
  val Inner, Left, Right, Full = Value
}

case class TableRef(tableName: String, colName: String, joinType: JoinType.Value = JoinType.Left)
case class Column(name: String,
                  expression: Option[String] = None,
                  dimensionRefs: Set[String] = Set(),
                  measureRefs: Set[String] = Set(),
                  tableRefs: Set[TableRef] = Set()) {

  // TODO: change this
  override def equals(obj: scala.Any) = super[Object].equals(obj)
}
