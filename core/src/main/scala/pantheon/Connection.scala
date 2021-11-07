package pantheon

import pantheon.backend.{Backend, BackendConnection}
import pantheon.planner.{Expression, Predicate, UnresolvedField}
import pantheon.schema._
import pantheon.util.Logging.LoggingContext
import cats.syntax.either._
import cats.syntax.traverse._
import cats.instances.either._
import cats.instances.option._
import pantheon.errors.{IncompatibleQueryException, InvalidQueryStructureException}

case class Metadata(fields: Map[String, Field])

class Connection(
    catalog: Catalog,
    val schemaName: String,
    val backend: Backend
) {

  @volatile
  private var backendConnectionInitialized = false

  lazy val backendConnection: BackendConnection = {
    backendConnectionInitialized = true
    backend.getConnection(schema)
  }

  lazy val schema: Schema = catalog
    .getSchema(schemaName)
    .getOrElse(throw new Exception(s"Schema $schemaName not found"))

  // Discuss: we could move more validations here and maybe create auxiliary lookup structures for planner on the way
  def createStatement(query: Query, params: Map[String, Expression] = Map.empty)(
      implicit ctx: LoggingContext): Statement = ctx.withSpan("Connection#createStatement") { _ =>
    Query.validate(query).foreach(err => throw new InvalidQueryStructureException(err))

    def measureExists(n: String) = !schema.isExcluded(n) && schema.getMeasure(n).isDefined
    def dimensionExists(n: String) = !schema.isExcluded(n) && schema.getDimensionAttribute(n).isDefined

    def validateMeasures(m: List[String]): Either[String, Unit] = {
      val absentMes = m.filterNot(measureExists)
      Either.cond(absentMes.isEmpty, (), s"measures [${absentMes.mkString(",")}] not found in schema")
    }
    def validateDimensions(d: List[String]): Either[String, Unit] = {
      val absentDims = d.filterNot(dimensionExists)
      Either.cond(absentDims.isEmpty, (), s"dimensions [${absentDims.mkString(",")}] not found in schema")
    }

    val (validateFilter, validateAggFilter) = {

      def validatePredicate(lbl: String, hasField: String => Boolean): Predicate => Either[String, Unit] = p => {
        val wrongFilterRefs = p.collect { case f: UnresolvedField if !hasField(f.name) => f.name }
        Either.cond(
          wrongFilterRefs.isEmpty,
          (),
          s"the following references ${wrongFilterRefs.mkString("(", ",", ")")} were not found in $lbl"
        )
      }

      validatePredicate("schema dimensions", dimensionExists) ->
        validatePredicate("schema fields", name => measureExists(name) || dimensionExists(name))
    }

    (query match {
      case q: AggregateQuery =>
        for {
          _ <- validateDimensions(q.rows ::: q.columns)
          _ <- validateMeasures(q.measures)
          _ <- q.filter.traverse(validateFilter)
          _ <- q.aggregateFilter.traverse(validateAggFilter)
        } yield
          if (q.pivoted) new PivotedStatement(this, q, params)
          else new AggregateStatement(this, q, params)

      case q: RecordQuery =>
        for {
          _ <- validateDimensions(q.rows)
          _ <- q.filter.traverse(validateFilter)
        } yield new RecordStatement(this, q, params)

      case q: SqlQuery =>
        Right(new SqlStatement(this, q.sql))
    }).valueOr(err => throw new IncompatibleQueryException(err, schema.name))
  }

  def getMetadata(fieldNames: List[String]): Metadata =
    Metadata(fieldNames.flatMap(f => schema.getField(f).map(f -> _))(collection.breakOut))

  def close(implicit ctx: LoggingContext): Unit = {
    if (backendConnectionInitialized) backendConnection.close
  }
}
