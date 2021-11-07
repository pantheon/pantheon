package services

import java.util.UUID

import slick.jdbc.{GetResult, JdbcProfile, PositionedParameters, PositionedResult}
import dao.Tables._
import pantheon.schema.{Schema, SchemaCompiler}

import scala.concurrent.{ExecutionContext, Future}
import SchemaRepo._
import pantheon.util._
import util.DBIOUtil._
import util.DBIOUtil.dbio.cond
import cats.implicits._
import cats.data.{EitherT, OptionT}
import errors.{ConcurrentModification, ConstraintViolation, RepoError}
import pantheon.DataSource
import pantheon.schema.SchemaAST
import pantheon.schema.parser.SchemaParser
import slick.basic.DatabaseConfig

object SchemaRepo {

  object ParsedPsl {
    def apply(rawPsl: String): Either[String, ParsedPsl] = SchemaParser(rawPsl).map(new ParsedPsl(rawPsl, _))
  }
  final class ParsedPsl private (val raw: String, val parsed: SchemaAST)

  type SchemaId = UUID
  def showKey(catId: UUID, schemaId: UUID) = s"Schema '$schemaId' from catalog '$catId'"
}

class SchemaRepo(dsRepo: DataSourceRepo)(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) {
  import dbConfig.db
  import profile.api._

  private def invalidPslInDb(name: String, error: String): Nothing =
    throw new AssertionError(
      s"Unexpected situation: invalid PSL in database, schemaName: '$name'." +
        s" Validation err:" + error
    )

  def list: Future[Seq[SchemasRow]] = db.run(Schemas.result)

  def list(catId: UUID): Future[Seq[SchemasRow]] = db.run(schemasByCatId(catId).result)

  def findAndCompile(catalogId: UUID, id: UUID): Future[Option[Schema]] =
    db.run(_findById(catalogId, id).semiflatMap(_compileRow).value)

  def findAndCompile(catId: UUID, name: String): Future[Option[Schema]] =
    db.run(_findByNameInCatalog(catId, name).semiflatMap(_compileRow).value)

  def getDataSourcesForSchemaName(catId: UUID, name: String): Future[Seq[DataSource]] = {
    def getDataSourcesQuery(dsIds: Set[UUID]): DBIO[Seq[DataSource]] =
      DataSources
        .filter(_.dataSourceId inSet dsIds)
        .join(DataSourceProducts)
        .on(_.dataSourceProductId === _.dataSourceProductId)
        .result
        .flatMap(v =>
          DBIO.sequence(v.map {
            case (ds, prod) => DBIO.from(dsRepo.createDataSourceUnsafe(ds, prod))
          }))

    val q = for {
      schema <- _findByNameInCatalog(catId, name).value
      ss <- DBIO.sequenceOption(schema.map(s => schemasRecursively(s.schemaId)))
      schemaIds = ss.toSet.flatten
      dsIds <- dataSourceIds(schemaIds)
      dataSources <- getDataSourcesQuery(dsIds.toSet)
    } yield dataSources
    db.run(q)
  }

  def find(catId: UUID, id: UUID): Future[Option[SchemasRow]] = db.run(_findById(catId, id).value)

  /**
    * It must not be possible to delete schema which is a child of other schema
    * Schema is removed along with all related SchemaUsages and DataSourceLinks
    */
  def delete(catId: UUID, id: UUID): Future[RepoError | Boolean] = {

    def delete =
      for {
        err <- parentsExistsCheck(id)
        res <- cond(
          err.isEmpty,
          rmAllParentSchemaUsages(id) >> rmAllDsLinks(id) >> schemaInCatalog(catId, id).delete.map(v => Right(v == 1)),
          Left(ConstraintViolation(s"Cannot delete ${showKey(catId, id)}.Reason: ${err.get}"))
        )
      } yield res

    schemaExistsInCatalog(catId, id).result
      .flatMap(dbio.cond(_, delete, Right(false)))
      .transactionally
      .run(db)
      .detectConstraintViolation(ConcurrentModification)
  }

  /**
    * It must not be possible to update catalogId of schema if catalog with given id does not exist (TODO: not checking this explicitly)
    * It must not be possible to change name or catalog id of schema if schema with the same name exists in catalog
    * It must not be possible to change name or catalog of schema which is a child of other schema
    * It must not be possible to replace PSL with corrupted one(containing non-existent datasources or schema names)
    * SchemaUsed and DatasourceUsed tables are modified accordingly if PSL is changed
    * Constraint #3 will not hold in concurrency scenario (there is no DB-level constraint!)
    */
  // DISCUSS: return Boolean on the right(call site has all needed data)
  def update(req: SchemasRow): Future[RepoError | Option[SchemasRow]] = {
    val (catId, id) = req.catalogId -> req.schemaId
    ParsedPsl(req.psl).fold(
      error => Future.successful(Left(ConstraintViolation(error))), { psl =>
        def _update(curRow: SchemasRow): DBIO[RepoError | SchemasRow] = {

          val nameDirty = curRow.name != psl.parsed.name
          val pslDirty = curRow.psl != req.psl

          // this description include only fields that may cause an error
          def changes = show(
            nameDirty.option(s"name '${curRow.name}' to '${psl.parsed.name}'") ++
              pslDirty.option("PSL")
          )

          def relink(reqSchema: SchemaAST): DBIO[Unit] = {
            DBIO.seq(
              rmChildSchemaUsages(curRow.catalogId, id),
              rmDsLinks(curRow.catalogId, id),
              addChildSchemaUsages(catId, id, getChildNames(reqSchema)),
              addDsLinks(catId, id, getDsNames(reqSchema))
            )
          }

          for {
            parentsExistsErr <- cond(nameDirty, parentsExistsCheck(id), None)

            nameExistsErr <- cond(
              nameDirty,
              schemaNameExistsChk(catId, psl.parsed.name),
              None
            )

            missingLinksErrs <- missingLinksCheck(catId, psl.parsed)

            constraintErrors = nameExistsErr ++ parentsExistsErr ++ missingLinksErrs

            reqSchemaCompilationErr <- cond(
              constraintErrors.isEmpty,
              schemaCompilesChk(psl.parsed, catId),
              None
            )

            errs = reqSchemaCompilationErr ++ constraintErrors
            res <- cond(
              errs.isEmpty,
              relink(psl.parsed) >> {
                val row = req.copy(name = psl.parsed.name)
                schemaInCatalog(catId, id).update(row).map(_ => Right(row))
              },
              Left(ConstraintViolation(s"Cannot change $changes in ${showKey(catId, id)}. Reasons: ${show(errs)}."))
            )
          } yield res
        }

        schemaInCatalogForUpd(catId, id).result.headOption
          .flatMap(_.traverse(v => EitherT(_update(v))).value)
          .transactionally
          .run(db)
          .detectConstraintViolation(ConcurrentModification)
      }
    )
  }

  /**
    * It must not be possible to create schema with non-existent catalogId
    * It must not be possible to create schema having existing name within catalogId
    * It must not be possible to create schema having corrupted PSL (containing non-existent datasources or schema names)
    * SchemaUsages and DataSourceLinks tables are populated when schema is created
    */
  def create(req: SchemasRow): Future[RepoError | SchemasRow] = {
    val (catId, id) = req.catalogId -> req.schemaId
    def createRelations(rs: SchemaAST, schemaId: UUID): DBIO[Unit] = {
      val (reqDss, reqChildren) = getDsNames(rs) -> getChildNames(rs)
      DBIO.seq(
        cond(reqChildren.nonEmpty, addChildSchemaUsages(req.catalogId, schemaId, reqChildren)),
        cond(reqDss.nonEmpty, addDsLinks(catId, schemaId, reqDss))
      )
    }

    ParsedPsl(req.psl).fold(
      error => Future.successful(Left(ConstraintViolation(error))),
      psl =>
        (for {
          schemaExistsErr <- noSchemaExistsChk(req.schemaId)
          catalogExistsErr <- catalogExistsChk(req.catalogId)
          schemaNameExistsErr <- dbio.cond(catalogExistsErr.isEmpty,
                                           schemaNameExistsChk(req.catalogId, psl.parsed.name),
                                           None)

          missingLinksErrs <- cond(catalogExistsErr.isEmpty, missingLinksCheck(catId, psl.parsed), Nil)

          constraintErrs = schemaExistsErr ++
            catalogExistsErr ++
            schemaNameExistsErr ++
            missingLinksErrs

          reqSchemaCompilationErrors <- cond(
            constraintErrs.isEmpty,
            schemaCompilesChk(psl.parsed, catId),
            None
          )

          errs = reqSchemaCompilationErrors ++ constraintErrs

          res <- cond(
            errs.isEmpty,
            (Schemas returning Schemas).+=(req.copy(name = psl.parsed.name)).map(Right(_)),
            Left(ConstraintViolation(s"Cannot create schema. Reason: ${show(errs)}"))
          )
          _ <- res.traverseDBIO(s => createRelations(psl.parsed, s.schemaId))
        } yield res).transactionally.run(db).detectConstraintViolation(ConcurrentModification)
    )
  }

  private def _findByNameInCatalog(catId: UUID, name: String): OptionT[profile.api.DBIO, SchemasRow] =
    OptionT[DBIO, SchemasRow](schemaInCatalogByName(catId, name).result.headOption)

  private def _findById(catId: UUID, id: UUID): OptionT[DBIO, SchemasRow] =
    OptionT[DBIO, SchemasRow](schemaInCatalog(catId, id).result.headOption)

  private def parseUnsafe(schemaName: String, psl: String): SchemaAST =
    SchemaParser(psl).fold(err => invalidPslInDb(schemaName, err), identity)

  private def _compileRow(row: SchemasRow): DBIO[Schema] =
    _compile(parseUnsafe(row.name, row.psl), row.catalogId)
      .map(_.fold(err => invalidPslInDb(row.name, err), identity))

  private def _compile(schema: SchemaAST, catId: UUID): DBIO[Either[String, Schema]] = {
    SchemaCompiler
      .compileParsed[DBIO](
        schema,
        schemaName => _findByNameInCatalog(catId, schemaName).map(v => parseUnsafe(schemaName, v.psl)).value,
        dsName =>
          dsRepo
            .getDataSourceQueryByName(catId, dsName)
            .map(_.getOrElse(throw new Exception(s"Data source $dsName not found in catalog id: $catId")))
      )
      .map(_.left.map(_.map(_.message).mkString(",")))
  }

  private def schemaCompilesChk(schema: SchemaAST, catalogId: UUID): DBIO[Option[String]] =
    _compile(schema, catalogId).map(_.left.toOption.map("Schema compilation errors:" + _))

  private val catalogById = Compiled((id: Rep[UUID]) => Catalogs.filter(_.catalogId === id))

  // using val instead of def in order to hide compiled query inside of the scope
  private val catalogExistsChk: UUID => DBIO[Option[String]] = {
    val c = Compiled((id: Rep[UUID]) => catalogById.extract(id).exists)
    id =>
      c(id).result.map(catExists => (!catExists).option(s"Catalog '$id' does not exist"))
  }
  private val noSchemaExistsChk: UUID => DBIO[Option[String]] = {
    val c = Compiled((id: Rep[UUID]) => Schemas.filter(_.schemaId === id).exists)
    id =>
      c(id).result.map(_.option(s"Schema '$id' already exists, schema id must be globally unique"))
  }

  private val parentsExistsCheck: SchemaId => DBIO[Option[String]] = {
    val c = Compiled(
      (schemaID: Rep[UUID]) =>
        SchemaUsages
          .filter(_.childId === schemaID)
          .join(Schemas)
          .on(_.parentId === _.schemaId)
          .map(_._2.name)
    )

    c(_).result.map(parents =>
      parents.nonEmpty.option(s"Schema has the child relation to the following parent schemas: ${show(parents)}"))
  }

  private def schemaNameExistsChk(catId: UUID, name: String): DBIO[Option[String]] =
    _findByNameInCatalog(catId, name).map(_ => s"Schema '$name' already exists within catalog '$catId'").value

  private def missingLinksCheck(catId: UUID, s: SchemaAST): DBIO[Iterable[String]] = {

    val childNames = getChildNames(s)
    val dsNames = getDsNames(s)

    for {
      children <- Schemas
        .filter(s => s.catalogId === catId && s.name.inSet(childNames))
        .map(_.name)
        .result
        .map(ns => childNames.diff(ns.toSet))

      dss <- DataSources
        .filter(ds => ds.catalogId === catId && ds.name.inSet(dsNames))
        .map(_.name)
        .result
        .map(ns => dsNames.diff(ns.toSet))

    } yield
      children.nonEmpty.option(s"Referenced children (${show(children)}) not found in catalog '$catId'") ++
        dss.nonEmpty.option(s"Referenced data sources (${show(dss)}) not found in catalog '$catId'")
  }

  private val schemaInCatalogByName = Compiled(
    (catalogId: Rep[UUID], name: Rep[String]) => Schemas.filter(s => s.name === name && s.catalogId === catalogId))

  private val schemaInCatalog = Compiled((catalogId: Rep[UUID], schemaId: Rep[UUID]) =>
    Schemas.filter(s => s.schemaId === schemaId && s.catalogId === catalogId))

  private val schemaExistsInCatalog = Compiled(schemaInCatalog.extract.tupled.andThen(_.length === 1))

  private val schemaInCatalogForUpd = Compiled(schemaInCatalog.extract.tupled.andThen(_.forUpdate))

  private val schemasByCatId = Compiled((catId: Rep[UUID]) => Schemas.filter(_.catalogId === catId))

  private def schemasRecursively(schemaId: UUID) = {
    sql"""with recursive nodes(schema_id) as (
               values(uuid(${schemaId.toString}))
               union
               select child_id from schema_usages, nodes where schema_usages.parent_id = nodes.schema_id
             )
             select schema_id from nodes"""
      .as[String]
      .map(_.map(UUID.fromString(_)))
  }

  private def dataSourceIds(schemaIds: Set[UUID]): DBIO[Seq[UUID]] =
    DataSourceLinks.filter(_.schemaId inSet schemaIds).map(_.dataSourceId).distinct.result

  private def rmChildSchemaUsages(catId: UUID, id: UUID): DBIO[Int] =
    SchemaUsages.filter(s => s.parentId === id).delete

  private def addChildSchemaUsages(reqCatId: UUID, parentId: UUID, childNames: Set[String]): DBIO[Unit] =
    for {
      childIds <- Schemas.filter(s => s.catalogId === reqCatId && s.name.inSet(childNames)).map(_.schemaId).result
      _ <- SchemaUsages ++= childIds.map(SchemaUsagesRow(parentId, _))
    } yield ()

  private def rmDsLinks(catId: UUID, id: UUID): DBIO[Int] =
    DataSourceLinks.filter(ds => ds.schemaId === id).delete

  private def addDsLinks(catId: UUID, schemaId: UUID, dsNames: Set[String]): DBIO[Unit] =
    for {
      dsIds <- DataSources.filter(s => s.catalogId === catId && s.name.inSet(dsNames)).map(_.dataSourceId).result
      _ <- DataSourceLinks ++= dsIds.map(DataSourceLinksRow(schemaId, _))
    } yield ()

  private val rmAllParentSchemaUsages: SchemaId => DBIO[Int] = {
    val c = Compiled((id: Rep[UUID]) => SchemaUsages.filter(_.parentId === id))
    c(_).delete
  }

  private val rmAllDsLinks: SchemaId => DBIO[Int] = {
    val c = Compiled((id: Rep[UUID]) => DataSourceLinks.filter(_.schemaId === id))
    c(_).delete
  }

  private def getDsNames(schema: SchemaAST): Set[String] =
    (schema.tables.flatMap(_.dataSource) ++ schema.dataSource).toSet
  private def getChildNames(schema: SchemaAST): Set[String] = schema.imports.map(_.name)(collection.breakOut)
}
