package dao
// AUTO-GENERATED Slick data model
object Tables {
  val profile: slick.jdbc.JdbcProfile = slick.jdbc.PostgresProfile
  import profile.api._

  import play.api.libs.json._
  import java.time.Instant
  import java.sql.Timestamp
  import slick.jdbc.PositionedResult
  import services.QueryHistoryRepo.{CompletionStatus, completionStatusFormat}
  import enumeratum.{EnumEntry, Enum}
  import scala.reflect.ClassTag

  def enumeratumColumnType[E <: EnumEntry: ClassTag](e: Enum[E]) =
    MappedColumnType.base[E, String](
      _.toString,
      e.withName
    )
  def enumColumnType[E <: Enumeration](e: E) = MappedColumnType.base[e.Value, String](
    _.toString,
    e.withName(_)
  )
  implicit val queryHistoryRecordTypeColumnType = enumeratumColumnType(management.QueryHistoryRecordType)
  implicit val queryTypeColumnType = enumColumnType(pantheon.QueryType)
  implicit val resourceTypeColumnType = enumeratumColumnType(services.Authorization.ResourceType)
  implicit val actionTypeColumnType = enumColumnType(services.Authorization.ActionType)
  implicit val mapColumnType = MappedColumnType.base[Map[String, String], String](
    { m =>
      Json.stringify(Json.toJson(m))
    }, { s =>
      Json.parse(s).as[Map[String, String]]
    }
  )
  implicit val completionStatusColumnType = MappedColumnType.base[CompletionStatus, String](
    { m =>
      Json.stringify(Json.toJson(m))
    }, { s =>
      Json.parse(s).as[CompletionStatus]
    }
  )
  implicit val instantColumnType = MappedColumnType.base[Instant, Timestamp](Timestamp.from(_), _.toInstant)

  // allow user code to use this as an implicit,
  // implicit GR[Map[String, String]] cannot be found outside Tables
  val mapImpl: slick.jdbc.GetResult[Map[String, String]] =
    (rs: PositionedResult) => Json.parse(rs.nextString).as[Map[String, String]]

  import slick.model.ForeignKeyAction
  // NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.
  import slick.jdbc.{GetResult => GR}

  /** DDL for all tables. Call .create to execute. */
  lazy val schema: profile.SchemaDescription = Array(
    BackendConfigs.schema,
    Catalogs.schema,
    Cols.schema,
    ConstraintColLists.schema,
    ConstraintCols.schema,
    DataSourceLinks.schema,
    DataSourceProductIcons.schema,
    DataSourceProductJars.schema,
    DataSourceProductProperties.schema,
    DataSourceProducts.schema,
    DataSources.schema,
    KeyConstraints.schema,
    PlayEvolutions.schema,
    PlayEvolutionsLock.schema,
    PrincipalPermissions.schema,
    QueryHistory.schema,
    RoleActions.schema,
    Roles.schema,
    SavedQueries.schema,
    Schemas.schema,
    SchemaUsages.schema,
    Tbls.schema
  ).reduceLeft(_ ++ _)
  @deprecated("Use .schema instead of .ddl", "3.0")
  def ddl = schema

  /** Entity class storing rows of table BackendConfigs
    *  @param name Database column name SqlType(varchar), Default(None)
    *  @param backendType Database column backend_type SqlType(varchar)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param params Database column params SqlType(varchar)
    *  @param catalogId Database column catalog_id SqlType(uuid)
    *  @param backendConfigId Database column backend_config_id SqlType(uuid), PrimaryKey */
  case class BackendConfigsRow(name: Option[String] = None,
                               backendType: String,
                               description: Option[String] = None,
                               params: Map[String, String],
                               catalogId: java.util.UUID,
                               backendConfigId: java.util.UUID)

  /** GetResult implicit for fetching BackendConfigsRow objects using plain SQL queries */
  implicit def GetResultBackendConfigsRow(implicit e0: GR[Option[String]],
                                          e1: GR[String],
                                          e2: GR[Map[String, String]],
                                          e3: GR[java.util.UUID]): GR[BackendConfigsRow] = GR { prs =>
    import prs._
    BackendConfigsRow.tupled(
      (<<?[String], <<[String], <<?[String], <<[Map[String, String]], <<[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table backend_configs. Objects of this class serve as prototypes for rows in queries. */
  class BackendConfigs(_tableTag: Tag) extends profile.api.Table[BackendConfigsRow](_tableTag, "backend_configs") {
    def * =
      (name, backendType, description, params, catalogId, backendConfigId) <> (BackendConfigsRow.tupled, BackendConfigsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (name, Rep.Some(backendType), description, Rep.Some(params), Rep.Some(catalogId), Rep.Some(backendConfigId)).shaped
        .<>(
          { r =>
            import r._; _2.map(_ => BackendConfigsRow.tupled((_1, _2.get, _3, _4.get, _5.get, _6.get)))
          },
          (_: Any) => throw new Exception("Inserting into ? projection not supported.")
        )

    /** Database column name SqlType(varchar), Default(None) */
    val name: Rep[Option[String]] = column[Option[String]]("name", O.Default(None))

    /** Database column backend_type SqlType(varchar) */
    val backendType: Rep[String] = column[String]("backend_type")

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column params SqlType(varchar) */
    val params: Rep[Map[String, String]] = column[Map[String, String]]("params")

    /** Database column catalog_id SqlType(uuid) */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id")

    /** Database column backend_config_id SqlType(uuid), PrimaryKey */
    val backendConfigId: Rep[java.util.UUID] = column[java.util.UUID]("backend_config_id", O.PrimaryKey)

    /** Foreign key referencing Catalogs (database name backend_configs_catalog_id_fkey) */
    lazy val catalogsFk = foreignKey("backend_configs_catalog_id_fkey", catalogId, Catalogs)(
      r => r.catalogId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table BackendConfigs */
  lazy val BackendConfigs = new TableQuery(tag => new BackendConfigs(tag))

  /** Entity class storing rows of table Catalogs
    *  @param name Database column name SqlType(varchar), Default(None)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param catalogId Database column catalog_id SqlType(uuid), PrimaryKey
    *  @param backendConfigId Database column backend_config_id SqlType(uuid), Default(None) */
  case class CatalogsRow(name: Option[String] = None,
                         description: Option[String] = None,
                         catalogId: java.util.UUID,
                         backendConfigId: Option[java.util.UUID] = None)

  /** GetResult implicit for fetching CatalogsRow objects using plain SQL queries */
  implicit def GetResultCatalogsRow(implicit e0: GR[Option[String]],
                                    e1: GR[java.util.UUID],
                                    e2: GR[Option[java.util.UUID]]): GR[CatalogsRow] = GR { prs =>
    import prs._
    CatalogsRow.tupled((<<?[String], <<?[String], <<[java.util.UUID], <<?[java.util.UUID]))
  }

  /** Table description of table catalogs. Objects of this class serve as prototypes for rows in queries. */
  class Catalogs(_tableTag: Tag) extends profile.api.Table[CatalogsRow](_tableTag, "catalogs") {
    def * = (name, description, catalogId, backendConfigId) <> (CatalogsRow.tupled, CatalogsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (name, description, Rep.Some(catalogId), backendConfigId).shaped.<>({ r =>
        import r._; _3.map(_ => CatalogsRow.tupled((_1, _2, _3.get, _4)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column name SqlType(varchar), Default(None) */
    val name: Rep[Option[String]] = column[Option[String]]("name", O.Default(None))

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column catalog_id SqlType(uuid), PrimaryKey */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id", O.PrimaryKey)

    /** Database column backend_config_id SqlType(uuid), Default(None) */
    val backendConfigId: Rep[Option[java.util.UUID]] =
      column[Option[java.util.UUID]]("backend_config_id", O.Default(None))

    /** Foreign key referencing BackendConfigs (database name catalogs_backend_config_id_fkey) */
    lazy val backendConfigsFk = foreignKey("catalogs_backend_config_id_fkey", backendConfigId, BackendConfigs)(
      r => Rep.Some(r.backendConfigId),
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table Catalogs */
  lazy val Catalogs = new TableQuery(tag => new Catalogs(tag))

  /** Entity class storing rows of table Cols
    *  @param tblColId Database column tbl_col_id SqlType(serial), AutoInc, PrimaryKey
    *  @param tblId Database column tbl_id SqlType(int4)
    *  @param name Database column name SqlType(varchar)
    *  @param displayName Database column display_name SqlType(varchar), Default(None)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param typ Database column typ SqlType(varchar) */
  case class ColsRow(tblColId: Int,
                     tblId: Int,
                     name: String,
                     displayName: Option[String] = None,
                     description: Option[String] = None,
                     typ: String)

  /** GetResult implicit for fetching ColsRow objects using plain SQL queries */
  implicit def GetResultColsRow(implicit e0: GR[Int], e1: GR[String], e2: GR[Option[String]]): GR[ColsRow] = GR { prs =>
    import prs._
    ColsRow.tupled((<<[Int], <<[Int], <<[String], <<?[String], <<?[String], <<[String]))
  }

  /** Table description of table cols. Objects of this class serve as prototypes for rows in queries. */
  class Cols(_tableTag: Tag) extends profile.api.Table[ColsRow](_tableTag, "cols") {
    def * = (tblColId, tblId, name, displayName, description, typ) <> (ColsRow.tupled, ColsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(tblColId), Rep.Some(tblId), Rep.Some(name), displayName, description, Rep.Some(typ)).shaped.<>(
        { r =>
          import r._; _1.map(_ => ColsRow.tupled((_1.get, _2.get, _3.get, _4, _5, _6.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column tbl_col_id SqlType(serial), AutoInc, PrimaryKey */
    val tblColId: Rep[Int] = column[Int]("tbl_col_id", O.AutoInc, O.PrimaryKey)

    /** Database column tbl_id SqlType(int4) */
    val tblId: Rep[Int] = column[Int]("tbl_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column display_name SqlType(varchar), Default(None) */
    val displayName: Rep[Option[String]] = column[Option[String]]("display_name", O.Default(None))

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column typ SqlType(varchar) */
    val typ: Rep[String] = column[String]("typ")

    /** Foreign key referencing Tbls (database name cols_tbl_id_fkey) */
    lazy val tblsFk = foreignKey("cols_tbl_id_fkey", tblId, Tbls)(r => r.tblId,
                                                                  onUpdate = ForeignKeyAction.NoAction,
                                                                  onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table Cols */
  lazy val Cols = new TableQuery(tag => new Cols(tag))

  /** Entity class storing rows of table ConstraintColLists
    *  @param colListId Database column col_list_id SqlType(serial), AutoInc, PrimaryKey */
  case class ConstraintColListsRow(colListId: Int)

  /** GetResult implicit for fetching ConstraintColListsRow objects using plain SQL queries */
  implicit def GetResultConstraintColListsRow(implicit e0: GR[Int]): GR[ConstraintColListsRow] = GR { prs =>
    import prs._
    ConstraintColListsRow(<<[Int])
  }

  /** Table description of table constraint_col_lists. Objects of this class serve as prototypes for rows in queries. */
  class ConstraintColLists(_tableTag: Tag)
      extends profile.api.Table[ConstraintColListsRow](_tableTag, "constraint_col_lists") {
    def * = colListId <> (ConstraintColListsRow, ConstraintColListsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      Rep
        .Some(colListId)
        .shaped
        .<>(r => r.map(_ => ConstraintColListsRow(r.get)),
            (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column col_list_id SqlType(serial), AutoInc, PrimaryKey */
    val colListId: Rep[Int] = column[Int]("col_list_id", O.AutoInc, O.PrimaryKey)
  }

  /** Collection-like TableQuery object for table ConstraintColLists */
  lazy val ConstraintColLists = new TableQuery(tag => new ConstraintColLists(tag))

  /** Entity class storing rows of table ConstraintCols
    *  @param colId Database column col_id SqlType(serial), AutoInc, PrimaryKey
    *  @param colListId Database column col_list_id SqlType(int4)
    *  @param name Database column name SqlType(varchar)
    *  @param idx Database column idx SqlType(int4) */
  case class ConstraintColsRow(colId: Int, colListId: Int, name: String, idx: Int)

  /** GetResult implicit for fetching ConstraintColsRow objects using plain SQL queries */
  implicit def GetResultConstraintColsRow(implicit e0: GR[Int], e1: GR[String]): GR[ConstraintColsRow] = GR { prs =>
    import prs._
    ConstraintColsRow.tupled((<<[Int], <<[Int], <<[String], <<[Int]))
  }

  /** Table description of table constraint_cols. Objects of this class serve as prototypes for rows in queries. */
  class ConstraintCols(_tableTag: Tag) extends profile.api.Table[ConstraintColsRow](_tableTag, "constraint_cols") {
    def * = (colId, colListId, name, idx) <> (ConstraintColsRow.tupled, ConstraintColsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(colId), Rep.Some(colListId), Rep.Some(name), Rep.Some(idx)).shaped.<>(
        { r =>
          import r._; _1.map(_ => ConstraintColsRow.tupled((_1.get, _2.get, _3.get, _4.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column col_id SqlType(serial), AutoInc, PrimaryKey */
    val colId: Rep[Int] = column[Int]("col_id", O.AutoInc, O.PrimaryKey)

    /** Database column col_list_id SqlType(int4) */
    val colListId: Rep[Int] = column[Int]("col_list_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column idx SqlType(int4) */
    val idx: Rep[Int] = column[Int]("idx")

    /** Foreign key referencing ConstraintColLists (database name constraint_cols_col_list_id_fkey) */
    lazy val constraintColListsFk = foreignKey("constraint_cols_col_list_id_fkey", colListId, ConstraintColLists)(
      r => r.colListId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table ConstraintCols */
  lazy val ConstraintCols = new TableQuery(tag => new ConstraintCols(tag))

  /** Entity class storing rows of table DataSourceLinks
    *  @param schemaId Database column schema_id SqlType(uuid)
    *  @param dataSourceId Database column data_source_id SqlType(uuid) */
  case class DataSourceLinksRow(schemaId: java.util.UUID, dataSourceId: java.util.UUID)

  /** GetResult implicit for fetching DataSourceLinksRow objects using plain SQL queries */
  implicit def GetResultDataSourceLinksRow(implicit e0: GR[java.util.UUID]): GR[DataSourceLinksRow] = GR { prs =>
    import prs._
    DataSourceLinksRow.tupled((<<[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table data_source_links. Objects of this class serve as prototypes for rows in queries. */
  class DataSourceLinks(_tableTag: Tag) extends profile.api.Table[DataSourceLinksRow](_tableTag, "data_source_links") {
    def * = (schemaId, dataSourceId) <> (DataSourceLinksRow.tupled, DataSourceLinksRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(schemaId), Rep.Some(dataSourceId)).shaped.<>({ r =>
        import r._; _1.map(_ => DataSourceLinksRow.tupled((_1.get, _2.get)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column schema_id SqlType(uuid) */
    val schemaId: Rep[java.util.UUID] = column[java.util.UUID]("schema_id")

    /** Database column data_source_id SqlType(uuid) */
    val dataSourceId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_id")

    /** Foreign key referencing DataSources (database name data_source_links_data_source_id_fkey) */
    lazy val dataSourcesFk = foreignKey("data_source_links_data_source_id_fkey", dataSourceId, DataSources)(
      r => r.dataSourceId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Schemas (database name data_source_links_schema_id_fkey) */
    lazy val schemasFk = foreignKey("data_source_links_schema_id_fkey", schemaId, Schemas)(
      r => r.schemaId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table DataSourceLinks */
  lazy val DataSourceLinks = new TableQuery(tag => new DataSourceLinks(tag))

  /** Entity class storing rows of table DataSourceProductIcons
    *  @param dataSourceProductId Database column data_source_product_id SqlType(uuid), PrimaryKey
    *  @param contents Database column contents SqlType(bytea) */
  case class DataSourceProductIconsRow(dataSourceProductId: java.util.UUID, contents: Array[Byte])

  /** GetResult implicit for fetching DataSourceProductIconsRow objects using plain SQL queries */
  implicit def GetResultDataSourceProductIconsRow(implicit e0: GR[java.util.UUID],
                                                  e1: GR[Array[Byte]]): GR[DataSourceProductIconsRow] = GR { prs =>
    import prs._
    DataSourceProductIconsRow.tupled((<<[java.util.UUID], <<[Array[Byte]]))
  }

  /** Table description of table data_source_product_icons. Objects of this class serve as prototypes for rows in queries. */
  class DataSourceProductIcons(_tableTag: Tag)
      extends profile.api.Table[DataSourceProductIconsRow](_tableTag, "data_source_product_icons") {
    def * = (dataSourceProductId, contents) <> (DataSourceProductIconsRow.tupled, DataSourceProductIconsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(dataSourceProductId), Rep.Some(contents)).shaped.<>({ r =>
        import r._; _1.map(_ => DataSourceProductIconsRow.tupled((_1.get, _2.get)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column data_source_product_id SqlType(uuid), PrimaryKey */
    val dataSourceProductId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_product_id", O.PrimaryKey)

    /** Database column contents SqlType(bytea) */
    val contents: Rep[Array[Byte]] = column[Array[Byte]]("contents")

    /** Foreign key referencing DataSourceProducts (database name data_source_product_icons_data_source_product_id_fkey) */
    lazy val dataSourceProductsFk =
      foreignKey("data_source_product_icons_data_source_product_id_fkey", dataSourceProductId, DataSourceProducts)(
        r => r.dataSourceProductId,
        onUpdate = ForeignKeyAction.NoAction,
        onDelete = ForeignKeyAction.Cascade)
  }

  /** Collection-like TableQuery object for table DataSourceProductIcons */
  lazy val DataSourceProductIcons = new TableQuery(tag => new DataSourceProductIcons(tag))

  /** Entity class storing rows of table DataSourceProductJars
    *  @param dataSourceProductId Database column data_source_product_id SqlType(uuid)
    *  @param name Database column name SqlType(varchar)
    *  @param content Database column content SqlType(bytea) */
  case class DataSourceProductJarsRow(dataSourceProductId: java.util.UUID, name: String, content: Array[Byte])

  /** GetResult implicit for fetching DataSourceProductJarsRow objects using plain SQL queries */
  implicit def GetResultDataSourceProductJarsRow(implicit e0: GR[java.util.UUID],
                                                 e1: GR[String],
                                                 e2: GR[Array[Byte]]): GR[DataSourceProductJarsRow] = GR { prs =>
    import prs._
    DataSourceProductJarsRow.tupled((<<[java.util.UUID], <<[String], <<[Array[Byte]]))
  }

  /** Table description of table data_source_product_jars. Objects of this class serve as prototypes for rows in queries. */
  class DataSourceProductJars(_tableTag: Tag)
      extends profile.api.Table[DataSourceProductJarsRow](_tableTag, "data_source_product_jars") {
    def * = (dataSourceProductId, name, content) <> (DataSourceProductJarsRow.tupled, DataSourceProductJarsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(dataSourceProductId), Rep.Some(name), Rep.Some(content)).shaped.<>(
        { r =>
          import r._; _1.map(_ => DataSourceProductJarsRow.tupled((_1.get, _2.get, _3.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column data_source_product_id SqlType(uuid) */
    val dataSourceProductId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_product_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column content SqlType(bytea) */
    val content: Rep[Array[Byte]] = column[Array[Byte]]("content")

    /** Primary key of DataSourceProductJars (database name jdbc_product_jars_pkey) */
    val pk = primaryKey("jdbc_product_jars_pkey", (dataSourceProductId, name))

    /** Foreign key referencing DataSourceProducts (database name jdbc_product_jars_jdbc_product_id_fkey) */
    lazy val dataSourceProductsFk =
      foreignKey("jdbc_product_jars_jdbc_product_id_fkey", dataSourceProductId, DataSourceProducts)(
        r => r.dataSourceProductId,
        onUpdate = ForeignKeyAction.NoAction,
        onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table DataSourceProductJars */
  lazy val DataSourceProductJars = new TableQuery(tag => new DataSourceProductJars(tag))

  /** Entity class storing rows of table DataSourceProductProperties
    *  @param dataSourceProductPropertyId Database column data_source_product_property_id SqlType(uuid), PrimaryKey
    *  @param dataSourceProductId Database column data_source_product_id SqlType(uuid)
    *  @param name Database column name SqlType(varchar)
    *  @param `type` Database column type SqlType(varchar)
    *  @param orderIndex Database column order_index SqlType(int4) */
  case class DataSourceProductPropertiesRow(dataSourceProductPropertyId: java.util.UUID,
                                            dataSourceProductId: java.util.UUID,
                                            name: String,
                                            `type`: String,
                                            orderIndex: Int)

  /** GetResult implicit for fetching DataSourceProductPropertiesRow objects using plain SQL queries */
  implicit def GetResultDataSourceProductPropertiesRow(implicit e0: GR[java.util.UUID],
                                                       e1: GR[String],
                                                       e2: GR[Int]): GR[DataSourceProductPropertiesRow] = GR { prs =>
    import prs._
    DataSourceProductPropertiesRow.tupled((<<[java.util.UUID], <<[java.util.UUID], <<[String], <<[String], <<[Int]))
  }

  /** Table description of table data_source_product_properties. Objects of this class serve as prototypes for rows in queries.
    *  NOTE: The following names collided with Scala keywords and were escaped: type */
  class DataSourceProductProperties(_tableTag: Tag)
      extends profile.api.Table[DataSourceProductPropertiesRow](_tableTag, "data_source_product_properties") {
    def * =
      (dataSourceProductPropertyId, dataSourceProductId, name, `type`, orderIndex) <> (DataSourceProductPropertiesRow.tupled, DataSourceProductPropertiesRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(dataSourceProductPropertyId),
       Rep.Some(dataSourceProductId),
       Rep.Some(name),
       Rep.Some(`type`),
       Rep.Some(orderIndex)).shaped.<>(
        { r =>
          import r._; _1.map(_ => DataSourceProductPropertiesRow.tupled((_1.get, _2.get, _3.get, _4.get, _5.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column data_source_product_property_id SqlType(uuid), PrimaryKey */
    val dataSourceProductPropertyId: Rep[java.util.UUID] =
      column[java.util.UUID]("data_source_product_property_id", O.PrimaryKey)

    /** Database column data_source_product_id SqlType(uuid) */
    val dataSourceProductId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_product_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column type SqlType(varchar)
      *  NOTE: The name was escaped because it collided with a Scala keyword. */
    val `type`: Rep[String] = column[String]("type")

    /** Database column order_index SqlType(int4) */
    val orderIndex: Rep[Int] = column[Int]("order_index")

    /** Foreign key referencing DataSourceProducts (database name data_source_product_properties_data_source_product_id_fkey) */
    lazy val dataSourceProductsFk =
      foreignKey("data_source_product_properties_data_source_product_id_fkey", dataSourceProductId, DataSourceProducts)(
        r => r.dataSourceProductId,
        onUpdate = ForeignKeyAction.NoAction,
        onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table DataSourceProductProperties */
  lazy val DataSourceProductProperties = new TableQuery(tag => new DataSourceProductProperties(tag))

  /** Entity class storing rows of table DataSourceProducts
    *  @param isBundled Database column is_bundled SqlType(bool), Default(false)
    *  @param productRoot Database column product_root SqlType(varchar)
    *  @param className Database column class_name SqlType(varchar), Default(None)
    *  @param dataSourceProductId Database column data_source_product_id SqlType(uuid), PrimaryKey
    *  @param name Database column name SqlType(varchar)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param `type` Database column type SqlType(varchar) */
  case class DataSourceProductsRow(isBundled: Boolean = false,
                                   productRoot: String,
                                   className: Option[String] = None,
                                   dataSourceProductId: java.util.UUID,
                                   name: String,
                                   description: Option[String] = None,
                                   `type`: String)

  /** GetResult implicit for fetching DataSourceProductsRow objects using plain SQL queries */
  implicit def GetResultDataSourceProductsRow(implicit e0: GR[Boolean],
                                              e1: GR[String],
                                              e2: GR[Option[String]],
                                              e3: GR[java.util.UUID]): GR[DataSourceProductsRow] = GR { prs =>
    import prs._
    DataSourceProductsRow.tupled(
      (<<[Boolean], <<[String], <<?[String], <<[java.util.UUID], <<[String], <<?[String], <<[String]))
  }

  /** Table description of table data_source_products. Objects of this class serve as prototypes for rows in queries.
    *  NOTE: The following names collided with Scala keywords and were escaped: type */
  class DataSourceProducts(_tableTag: Tag)
      extends profile.api.Table[DataSourceProductsRow](_tableTag, "data_source_products") {
    def * =
      (isBundled, productRoot, className, dataSourceProductId, name, description, `type`) <> (DataSourceProductsRow.tupled, DataSourceProductsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(isBundled),
       Rep.Some(productRoot),
       className,
       Rep.Some(dataSourceProductId),
       Rep.Some(name),
       description,
       Rep.Some(`type`)).shaped.<>(
        { r =>
          import r._; _1.map(_ => DataSourceProductsRow.tupled((_1.get, _2.get, _3, _4.get, _5.get, _6, _7.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column is_bundled SqlType(bool), Default(false) */
    val isBundled: Rep[Boolean] = column[Boolean]("is_bundled", O.Default(false))

    /** Database column product_root SqlType(varchar) */
    val productRoot: Rep[String] = column[String]("product_root")

    /** Database column class_name SqlType(varchar), Default(None) */
    val className: Rep[Option[String]] = column[Option[String]]("class_name", O.Default(None))

    /** Database column data_source_product_id SqlType(uuid), PrimaryKey */
    val dataSourceProductId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_product_id", O.PrimaryKey)

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column type SqlType(varchar)
      *  NOTE: The name was escaped because it collided with a Scala keyword. */
    val `type`: Rep[String] = column[String]("type")
  }

  /** Collection-like TableQuery object for table DataSourceProducts */
  lazy val DataSourceProducts = new TableQuery(tag => new DataSourceProducts(tag))

  /** Entity class storing rows of table DataSources
    *  @param name Database column name SqlType(varchar)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param properties Database column properties SqlType(varchar)
    *  @param catalogId Database column catalog_id SqlType(uuid)
    *  @param dataSourceProductId Database column data_source_product_id SqlType(uuid)
    *  @param dataSourceId Database column data_source_id SqlType(uuid), PrimaryKey */
  case class DataSourcesRow(name: String,
                            description: Option[String] = None,
                            properties: Map[String, String],
                            catalogId: java.util.UUID,
                            dataSourceProductId: java.util.UUID,
                            dataSourceId: java.util.UUID)

  /** GetResult implicit for fetching DataSourcesRow objects using plain SQL queries */
  implicit def GetResultDataSourcesRow(implicit e0: GR[String],
                                       e1: GR[Option[String]],
                                       e2: GR[Map[String, String]],
                                       e3: GR[java.util.UUID]): GR[DataSourcesRow] = GR { prs =>
    import prs._
    DataSourcesRow.tupled(
      (<<[String], <<?[String], <<[Map[String, String]], <<[java.util.UUID], <<[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table data_sources. Objects of this class serve as prototypes for rows in queries. */
  class DataSources(_tableTag: Tag) extends profile.api.Table[DataSourcesRow](_tableTag, "data_sources") {
    def * =
      (name, description, properties, catalogId, dataSourceProductId, dataSourceId) <> (DataSourcesRow.tupled, DataSourcesRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(name),
       description,
       Rep.Some(properties),
       Rep.Some(catalogId),
       Rep.Some(dataSourceProductId),
       Rep.Some(dataSourceId)).shaped.<>(
        { r =>
          import r._; _1.map(_ => DataSourcesRow.tupled((_1.get, _2, _3.get, _4.get, _5.get, _6.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column properties SqlType(varchar) */
    val properties: Rep[Map[String, String]] = column[Map[String, String]]("properties")

    /** Database column catalog_id SqlType(uuid) */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id")

    /** Database column data_source_product_id SqlType(uuid) */
    val dataSourceProductId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_product_id")

    /** Database column data_source_id SqlType(uuid), PrimaryKey */
    val dataSourceId: Rep[java.util.UUID] = column[java.util.UUID]("data_source_id", O.PrimaryKey)

    /** Foreign key referencing Catalogs (database name data_sources_catalog_id_fkey) */
    lazy val catalogsFk = foreignKey("data_sources_catalog_id_fkey", catalogId, Catalogs)(
      r => r.catalogId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing DataSourceProducts (database name data_sources_jdbc_product_id_fkey) */
    lazy val dataSourceProductsFk =
      foreignKey("data_sources_jdbc_product_id_fkey", dataSourceProductId, DataSourceProducts)(
        r => r.dataSourceProductId,
        onUpdate = ForeignKeyAction.NoAction,
        onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table DataSources */
  lazy val DataSources = new TableQuery(tag => new DataSources(tag))

  /** Entity class storing rows of table KeyConstraints
    *  @param keyConstraintId Database column key_constraint_id SqlType(serial), AutoInc, PrimaryKey
    *  @param childColListId Database column child_col_list_id SqlType(int4)
    *  @param childTblId Database column child_tbl_id SqlType(int4)
    *  @param parentColListId Database column parent_col_list_id SqlType(int4)
    *  @param parentTblId Database column parent_tbl_id SqlType(int4)
    *  @param name Database column name SqlType(varchar)
    *  @param typ Database column typ SqlType(varchar) */
  case class KeyConstraintsRow(keyConstraintId: Int,
                               childColListId: Int,
                               childTblId: Int,
                               parentColListId: Int,
                               parentTblId: Int,
                               name: String,
                               typ: String)

  /** GetResult implicit for fetching KeyConstraintsRow objects using plain SQL queries */
  implicit def GetResultKeyConstraintsRow(implicit e0: GR[Int], e1: GR[String]): GR[KeyConstraintsRow] = GR { prs =>
    import prs._
    KeyConstraintsRow.tupled((<<[Int], <<[Int], <<[Int], <<[Int], <<[Int], <<[String], <<[String]))
  }

  /** Table description of table key_constraints. Objects of this class serve as prototypes for rows in queries. */
  class KeyConstraints(_tableTag: Tag) extends profile.api.Table[KeyConstraintsRow](_tableTag, "key_constraints") {
    def * =
      (keyConstraintId, childColListId, childTblId, parentColListId, parentTblId, name, typ) <> (KeyConstraintsRow.tupled, KeyConstraintsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(keyConstraintId),
       Rep.Some(childColListId),
       Rep.Some(childTblId),
       Rep.Some(parentColListId),
       Rep.Some(parentTblId),
       Rep.Some(name),
       Rep.Some(typ)).shaped.<>(
        { r =>
          import r._; _1.map(_ => KeyConstraintsRow.tupled((_1.get, _2.get, _3.get, _4.get, _5.get, _6.get, _7.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column key_constraint_id SqlType(serial), AutoInc, PrimaryKey */
    val keyConstraintId: Rep[Int] = column[Int]("key_constraint_id", O.AutoInc, O.PrimaryKey)

    /** Database column child_col_list_id SqlType(int4) */
    val childColListId: Rep[Int] = column[Int]("child_col_list_id")

    /** Database column child_tbl_id SqlType(int4) */
    val childTblId: Rep[Int] = column[Int]("child_tbl_id")

    /** Database column parent_col_list_id SqlType(int4) */
    val parentColListId: Rep[Int] = column[Int]("parent_col_list_id")

    /** Database column parent_tbl_id SqlType(int4) */
    val parentTblId: Rep[Int] = column[Int]("parent_tbl_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column typ SqlType(varchar) */
    val typ: Rep[String] = column[String]("typ")

    /** Foreign key referencing ConstraintColLists (database name key_constraints_child_col_list_id_fkey) */
    lazy val constraintColListsFk1 = foreignKey(
      "key_constraints_child_col_list_id_fkey",
      childColListId,
      ConstraintColLists)(r => r.colListId, onUpdate = ForeignKeyAction.NoAction, onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing ConstraintColLists (database name key_constraints_parent_col_list_id_fkey) */
    lazy val constraintColListsFk2 = foreignKey(
      "key_constraints_parent_col_list_id_fkey",
      parentColListId,
      ConstraintColLists)(r => r.colListId, onUpdate = ForeignKeyAction.NoAction, onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Tbls (database name key_constraints_child_tbl_id_fkey) */
    lazy val tblsFk3 = foreignKey("key_constraints_child_tbl_id_fkey", childTblId, Tbls)(
      r => r.tblId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Tbls (database name key_constraints_parent_tbl_id_fkey) */
    lazy val tblsFk4 = foreignKey("key_constraints_parent_tbl_id_fkey", parentTblId, Tbls)(
      r => r.tblId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table KeyConstraints */
  lazy val KeyConstraints = new TableQuery(tag => new KeyConstraints(tag))

  /** Entity class storing rows of table PlayEvolutions
    *  @param id Database column id SqlType(int4), PrimaryKey
    *  @param hash Database column hash SqlType(varchar), Length(255,true)
    *  @param appliedAt Database column applied_at SqlType(timestamp)
    *  @param applyScript Database column apply_script SqlType(text), Default(None)
    *  @param revertScript Database column revert_script SqlType(text), Default(None)
    *  @param state Database column state SqlType(varchar), Length(255,true), Default(None)
    *  @param lastProblem Database column last_problem SqlType(text), Default(None) */
  case class PlayEvolutionsRow(id: Int,
                               hash: String,
                               appliedAt: Instant,
                               applyScript: Option[String] = None,
                               revertScript: Option[String] = None,
                               state: Option[String] = None,
                               lastProblem: Option[String] = None)

  /** GetResult implicit for fetching PlayEvolutionsRow objects using plain SQL queries */
  implicit def GetResultPlayEvolutionsRow(implicit e0: GR[Int],
                                          e1: GR[String],
                                          e2: GR[Instant],
                                          e3: GR[Option[String]]): GR[PlayEvolutionsRow] = GR { prs =>
    import prs._
    PlayEvolutionsRow.tupled((<<[Int], <<[String], <<[Instant], <<?[String], <<?[String], <<?[String], <<?[String]))
  }

  /** Table description of table play_evolutions. Objects of this class serve as prototypes for rows in queries. */
  class PlayEvolutions(_tableTag: Tag) extends profile.api.Table[PlayEvolutionsRow](_tableTag, "play_evolutions") {
    def * =
      (id, hash, appliedAt, applyScript, revertScript, state, lastProblem) <> (PlayEvolutionsRow.tupled, PlayEvolutionsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(id), Rep.Some(hash), Rep.Some(appliedAt), applyScript, revertScript, state, lastProblem).shaped.<>(
        { r =>
          import r._; _1.map(_ => PlayEvolutionsRow.tupled((_1.get, _2.get, _3.get, _4, _5, _6, _7)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column id SqlType(int4), PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.PrimaryKey)

    /** Database column hash SqlType(varchar), Length(255,true) */
    val hash: Rep[String] = column[String]("hash", O.Length(255, varying = true))

    /** Database column applied_at SqlType(timestamp) */
    val appliedAt: Rep[Instant] = column[Instant]("applied_at")

    /** Database column apply_script SqlType(text), Default(None) */
    val applyScript: Rep[Option[String]] = column[Option[String]]("apply_script", O.Default(None))

    /** Database column revert_script SqlType(text), Default(None) */
    val revertScript: Rep[Option[String]] = column[Option[String]]("revert_script", O.Default(None))

    /** Database column state SqlType(varchar), Length(255,true), Default(None) */
    val state: Rep[Option[String]] = column[Option[String]]("state", O.Length(255, varying = true), O.Default(None))

    /** Database column last_problem SqlType(text), Default(None) */
    val lastProblem: Rep[Option[String]] = column[Option[String]]("last_problem", O.Default(None))
  }

  /** Collection-like TableQuery object for table PlayEvolutions */
  lazy val PlayEvolutions = new TableQuery(tag => new PlayEvolutions(tag))

  /** Entity class storing rows of table PlayEvolutionsLock
    *  @param lock Database column lock SqlType(int4), PrimaryKey */
  case class PlayEvolutionsLockRow(lock: Int)

  /** GetResult implicit for fetching PlayEvolutionsLockRow objects using plain SQL queries */
  implicit def GetResultPlayEvolutionsLockRow(implicit e0: GR[Int]): GR[PlayEvolutionsLockRow] = GR { prs =>
    import prs._
    PlayEvolutionsLockRow(<<[Int])
  }

  /** Table description of table play_evolutions_lock. Objects of this class serve as prototypes for rows in queries. */
  class PlayEvolutionsLock(_tableTag: Tag)
      extends profile.api.Table[PlayEvolutionsLockRow](_tableTag, "play_evolutions_lock") {
    def * = lock <> (PlayEvolutionsLockRow, PlayEvolutionsLockRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      Rep
        .Some(lock)
        .shaped
        .<>(r => r.map(_ => PlayEvolutionsLockRow(r.get)),
            (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column lock SqlType(int4), PrimaryKey */
    val lock: Rep[Int] = column[Int]("lock", O.PrimaryKey)
  }

  /** Collection-like TableQuery object for table PlayEvolutionsLock */
  lazy val PlayEvolutionsLock = new TableQuery(tag => new PlayEvolutionsLock(tag))

  /** Entity class storing rows of table PrincipalPermissions
    *  @param permissionId Database column permission_id SqlType(uuid), PrimaryKey
    *  @param principalId Database column principal_id SqlType(uuid)
    *  @param roleId Database column role_id SqlType(uuid)
    *  @param resourceId Database column resource_id SqlType(uuid) */
  case class PrincipalPermissionsRow(permissionId: java.util.UUID,
                                     principalId: java.util.UUID,
                                     roleId: java.util.UUID,
                                     resourceId: java.util.UUID)

  /** GetResult implicit for fetching PrincipalPermissionsRow objects using plain SQL queries */
  implicit def GetResultPrincipalPermissionsRow(implicit e0: GR[java.util.UUID]): GR[PrincipalPermissionsRow] = GR {
    prs =>
      import prs._
      PrincipalPermissionsRow.tupled((<<[java.util.UUID], <<[java.util.UUID], <<[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table principal_permissions. Objects of this class serve as prototypes for rows in queries. */
  class PrincipalPermissions(_tableTag: Tag)
      extends profile.api.Table[PrincipalPermissionsRow](_tableTag, "principal_permissions") {
    def * =
      (permissionId, principalId, roleId, resourceId) <> (PrincipalPermissionsRow.tupled, PrincipalPermissionsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(permissionId), Rep.Some(principalId), Rep.Some(roleId), Rep.Some(resourceId)).shaped.<>(
        { r =>
          import r._; _1.map(_ => PrincipalPermissionsRow.tupled((_1.get, _2.get, _3.get, _4.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column permission_id SqlType(uuid), PrimaryKey */
    val permissionId: Rep[java.util.UUID] = column[java.util.UUID]("permission_id", O.PrimaryKey)

    /** Database column principal_id SqlType(uuid) */
    val principalId: Rep[java.util.UUID] = column[java.util.UUID]("principal_id")

    /** Database column role_id SqlType(uuid) */
    val roleId: Rep[java.util.UUID] = column[java.util.UUID]("role_id")

    /** Database column resource_id SqlType(uuid) */
    val resourceId: Rep[java.util.UUID] = column[java.util.UUID]("resource_id")

    /** Foreign key referencing Roles (database name principal_permissions_role_id_fkey) */
    lazy val rolesFk = foreignKey("principal_permissions_role_id_fkey", roleId, Roles)(
      r => r.roleId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table PrincipalPermissions */
  lazy val PrincipalPermissions = new TableQuery(tag => new PrincipalPermissions(tag))

  /** Entity class storing rows of table QueryHistory
    *  @param queryHistoryId Database column query_history_id SqlType(uuid), PrimaryKey
    *  @param startedAt Database column started_at SqlType(timestamp)
    *  @param query Database column query SqlType(varchar)
    *  @param completionStatus Database column completion_status SqlType(varchar), Default(None)
    *  @param completedAt Database column completed_at SqlType(timestamp), Default(None)
    *  @param plan Database column plan SqlType(varchar), Default(None)
    *  @param backendLogicalPlan Database column backend_logical_plan SqlType(varchar), Default(None)
    *  @param backendPhysicalPlan Database column backend_physical_plan SqlType(varchar), Default(None)
    *  @param catalogId Database column catalog_id SqlType(uuid)
    *  @param customReference Database column custom_reference SqlType(varchar), Default(None)
    *  @param dataSourceId Database column data_source_id SqlType(uuid), Default(None)
    *  @param schemaId Database column schema_id SqlType(uuid), Default(None)
    *  @param `type` Database column type SqlType(varchar) */
  case class QueryHistoryRow(queryHistoryId: java.util.UUID,
                             startedAt: Instant,
                             query: String,
                             completionStatus: Option[CompletionStatus] = None,
                             completedAt: Option[Instant] = None,
                             plan: Option[String] = None,
                             backendLogicalPlan: Option[String] = None,
                             backendPhysicalPlan: Option[String] = None,
                             catalogId: java.util.UUID,
                             customReference: Option[String] = None,
                             dataSourceId: Option[java.util.UUID] = None,
                             schemaId: Option[java.util.UUID] = None,
                             `type`: management.QueryHistoryRecordType)

  /** GetResult implicit for fetching QueryHistoryRow objects using plain SQL queries */
  implicit def GetResultQueryHistoryRow(implicit e0: GR[java.util.UUID],
                                        e1: GR[Instant],
                                        e2: GR[String],
                                        e3: GR[Option[CompletionStatus]],
                                        e4: GR[Option[Instant]],
                                        e5: GR[Option[String]],
                                        e6: GR[Option[java.util.UUID]],
                                        e7: GR[management.QueryHistoryRecordType]): GR[QueryHistoryRow] = GR { prs =>
    import prs._
    QueryHistoryRow.tupled(
      (<<[java.util.UUID],
       <<[Instant],
       <<[String],
       <<?[CompletionStatus],
       <<?[Instant],
       <<?[String],
       <<?[String],
       <<?[String],
       <<[java.util.UUID],
       <<?[String],
       <<?[java.util.UUID],
       <<?[java.util.UUID],
       <<[management.QueryHistoryRecordType]))
  }

  /** Table description of table query_history. Objects of this class serve as prototypes for rows in queries.
    *  NOTE: The following names collided with Scala keywords and were escaped: type */
  class QueryHistory(_tableTag: Tag) extends profile.api.Table[QueryHistoryRow](_tableTag, "query_history") {
    def * =
      (queryHistoryId,
       startedAt,
       query,
       completionStatus,
       completedAt,
       plan,
       backendLogicalPlan,
       backendPhysicalPlan,
       catalogId,
       customReference,
       dataSourceId,
       schemaId,
       `type`) <> (QueryHistoryRow.tupled, QueryHistoryRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(queryHistoryId),
       Rep.Some(startedAt),
       Rep.Some(query),
       completionStatus,
       completedAt,
       plan,
       backendLogicalPlan,
       backendPhysicalPlan,
       Rep.Some(catalogId),
       customReference,
       dataSourceId,
       schemaId,
       Rep.Some(`type`)).shaped.<>(
        { r =>
          import r._;
          _1.map(_ =>
            QueryHistoryRow.tupled((_1.get, _2.get, _3.get, _4, _5, _6, _7, _8, _9.get, _10, _11, _12, _13.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column query_history_id SqlType(uuid), PrimaryKey */
    val queryHistoryId: Rep[java.util.UUID] = column[java.util.UUID]("query_history_id", O.PrimaryKey)

    /** Database column started_at SqlType(timestamp) */
    val startedAt: Rep[Instant] = column[Instant]("started_at")

    /** Database column query SqlType(varchar) */
    val query: Rep[String] = column[String]("query")

    /** Database column completion_status SqlType(varchar), Default(None) */
    val completionStatus: Rep[Option[CompletionStatus]] =
      column[Option[CompletionStatus]]("completion_status", O.Default(None))

    /** Database column completed_at SqlType(timestamp), Default(None) */
    val completedAt: Rep[Option[Instant]] = column[Option[Instant]]("completed_at", O.Default(None))

    /** Database column plan SqlType(varchar), Default(None) */
    val plan: Rep[Option[String]] = column[Option[String]]("plan", O.Default(None))

    /** Database column backend_logical_plan SqlType(varchar), Default(None) */
    val backendLogicalPlan: Rep[Option[String]] = column[Option[String]]("backend_logical_plan", O.Default(None))

    /** Database column backend_physical_plan SqlType(varchar), Default(None) */
    val backendPhysicalPlan: Rep[Option[String]] = column[Option[String]]("backend_physical_plan", O.Default(None))

    /** Database column catalog_id SqlType(uuid) */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id")

    /** Database column custom_reference SqlType(varchar), Default(None) */
    val customReference: Rep[Option[String]] = column[Option[String]]("custom_reference", O.Default(None))

    /** Database column data_source_id SqlType(uuid), Default(None) */
    val dataSourceId: Rep[Option[java.util.UUID]] = column[Option[java.util.UUID]]("data_source_id", O.Default(None))

    /** Database column schema_id SqlType(uuid), Default(None) */
    val schemaId: Rep[Option[java.util.UUID]] = column[Option[java.util.UUID]]("schema_id", O.Default(None))

    /** Database column type SqlType(varchar)
      *  NOTE: The name was escaped because it collided with a Scala keyword. */
    val `type`: Rep[management.QueryHistoryRecordType] = column[management.QueryHistoryRecordType]("type")

    /** Foreign key referencing Catalogs (database name query_history_catalog_id_fkey) */
    lazy val catalogsFk = foreignKey("query_history_catalog_id_fkey", catalogId, Catalogs)(
      r => r.catalogId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table QueryHistory */
  lazy val QueryHistory = new TableQuery(tag => new QueryHistory(tag))

  /** Entity class storing rows of table RoleActions
    *  @param roleId Database column role_id SqlType(uuid)
    *  @param actionType Database column action_type SqlType(varchar) */
  case class RoleActionsRow(roleId: java.util.UUID, actionType: services.Authorization.ActionType.Value)

  /** GetResult implicit for fetching RoleActionsRow objects using plain SQL queries */
  implicit def GetResultRoleActionsRow(implicit e0: GR[java.util.UUID],
                                       e1: GR[services.Authorization.ActionType.Value]): GR[RoleActionsRow] = GR {
    prs =>
      import prs._
      RoleActionsRow.tupled((<<[java.util.UUID], <<[services.Authorization.ActionType.Value]))
  }

  /** Table description of table role_actions. Objects of this class serve as prototypes for rows in queries. */
  class RoleActions(_tableTag: Tag) extends profile.api.Table[RoleActionsRow](_tableTag, "role_actions") {
    def * = (roleId, actionType) <> (RoleActionsRow.tupled, RoleActionsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(roleId), Rep.Some(actionType)).shaped.<>({ r =>
        import r._; _1.map(_ => RoleActionsRow.tupled((_1.get, _2.get)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column role_id SqlType(uuid) */
    val roleId: Rep[java.util.UUID] = column[java.util.UUID]("role_id")

    /** Database column action_type SqlType(varchar) */
    val actionType: Rep[services.Authorization.ActionType.Value] =
      column[services.Authorization.ActionType.Value]("action_type")

    /** Primary key of RoleActions (database name role_actions_pkey) */
    val pk = primaryKey("role_actions_pkey", (roleId, actionType))

    /** Foreign key referencing Roles (database name role_actions_role_id_fkey) */
    lazy val rolesFk = foreignKey("role_actions_role_id_fkey", roleId, Roles)(r => r.roleId,
                                                                              onUpdate = ForeignKeyAction.NoAction,
                                                                              onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table RoleActions */
  lazy val RoleActions = new TableQuery(tag => new RoleActions(tag))

  /** Entity class storing rows of table Roles
    *  @param roleId Database column role_id SqlType(uuid), PrimaryKey
    *  @param resourceType Database column resource_type SqlType(varchar)
    *  @param name Database column name SqlType(varchar) */
  case class RolesRow(roleId: java.util.UUID, resourceType: services.Authorization.ResourceType, name: String)

  /** GetResult implicit for fetching RolesRow objects using plain SQL queries */
  implicit def GetResultRolesRow(implicit e0: GR[java.util.UUID],
                                 e1: GR[services.Authorization.ResourceType],
                                 e2: GR[String]): GR[RolesRow] = GR { prs =>
    import prs._
    RolesRow.tupled((<<[java.util.UUID], <<[services.Authorization.ResourceType], <<[String]))
  }

  /** Table description of table roles. Objects of this class serve as prototypes for rows in queries. */
  class Roles(_tableTag: Tag) extends profile.api.Table[RolesRow](_tableTag, "roles") {
    def * = (roleId, resourceType, name) <> (RolesRow.tupled, RolesRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(roleId), Rep.Some(resourceType), Rep.Some(name)).shaped.<>({ r =>
        import r._; _1.map(_ => RolesRow.tupled((_1.get, _2.get, _3.get)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column role_id SqlType(uuid), PrimaryKey */
    val roleId: Rep[java.util.UUID] = column[java.util.UUID]("role_id", O.PrimaryKey)

    /** Database column resource_type SqlType(varchar) */
    val resourceType: Rep[services.Authorization.ResourceType] =
      column[services.Authorization.ResourceType]("resource_type")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")
  }

  /** Collection-like TableQuery object for table Roles */
  lazy val Roles = new TableQuery(tag => new Roles(tag))

  /** Entity class storing rows of table SavedQueries
    *  @param schemaId Database column schema_id SqlType(uuid)
    *  @param query Database column query SqlType(varchar)
    *  @param savedQueryId Database column saved_query_id SqlType(uuid), PrimaryKey
    *  @param catalogId Database column catalog_id SqlType(uuid)
    *  @param name Database column name SqlType(varchar), Default(None)
    *  @param description Database column description SqlType(varchar), Default(None) */
  case class SavedQueriesRow(schemaId: java.util.UUID,
                             query: String,
                             savedQueryId: java.util.UUID,
                             catalogId: java.util.UUID,
                             name: Option[String] = None,
                             description: Option[String] = None)

  /** GetResult implicit for fetching SavedQueriesRow objects using plain SQL queries */
  implicit def GetResultSavedQueriesRow(implicit e0: GR[java.util.UUID],
                                        e1: GR[String],
                                        e2: GR[Option[String]]): GR[SavedQueriesRow] = GR { prs =>
    import prs._
    SavedQueriesRow.tupled(
      (<<[java.util.UUID], <<[String], <<[java.util.UUID], <<[java.util.UUID], <<?[String], <<?[String]))
  }

  /** Table description of table saved_queries. Objects of this class serve as prototypes for rows in queries. */
  class SavedQueries(_tableTag: Tag) extends profile.api.Table[SavedQueriesRow](_tableTag, "saved_queries") {
    def * =
      (schemaId, query, savedQueryId, catalogId, name, description) <> (SavedQueriesRow.tupled, SavedQueriesRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(schemaId), Rep.Some(query), Rep.Some(savedQueryId), Rep.Some(catalogId), name, description).shaped.<>(
        { r =>
          import r._; _1.map(_ => SavedQueriesRow.tupled((_1.get, _2.get, _3.get, _4.get, _5, _6)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column schema_id SqlType(uuid) */
    val schemaId: Rep[java.util.UUID] = column[java.util.UUID]("schema_id")

    /** Database column query SqlType(varchar) */
    val query: Rep[String] = column[String]("query")

    /** Database column saved_query_id SqlType(uuid), PrimaryKey */
    val savedQueryId: Rep[java.util.UUID] = column[java.util.UUID]("saved_query_id", O.PrimaryKey)

    /** Database column catalog_id SqlType(uuid) */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id")

    /** Database column name SqlType(varchar), Default(None) */
    val name: Rep[Option[String]] = column[Option[String]]("name", O.Default(None))

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Foreign key referencing Catalogs (database name saved_queries_catalog_id_fkey) */
    lazy val catalogsFk = foreignKey("saved_queries_catalog_id_fkey", catalogId, Catalogs)(
      r => r.catalogId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Schemas (database name fk_schema_id) */
    lazy val schemasFk = foreignKey("fk_schema_id", schemaId, Schemas)(r => r.schemaId,
                                                                       onUpdate = ForeignKeyAction.NoAction,
                                                                       onDelete = ForeignKeyAction.Cascade)
  }

  /** Collection-like TableQuery object for table SavedQueries */
  lazy val SavedQueries = new TableQuery(tag => new SavedQueries(tag))

  /** Entity class storing rows of table Schemas
    *  @param name Database column name SqlType(varchar)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param psl Database column psl SqlType(varchar)
    *  @param catalogId Database column catalog_id SqlType(uuid)
    *  @param backendConfigId Database column backend_config_id SqlType(uuid), Default(None)
    *  @param schemaId Database column schema_id SqlType(uuid), PrimaryKey */
  case class SchemasRow(name: String,
                        description: Option[String] = None,
                        psl: String,
                        catalogId: java.util.UUID,
                        backendConfigId: Option[java.util.UUID] = None,
                        schemaId: java.util.UUID)

  /** GetResult implicit for fetching SchemasRow objects using plain SQL queries */
  implicit def GetResultSchemasRow(implicit e0: GR[String],
                                   e1: GR[Option[String]],
                                   e2: GR[java.util.UUID],
                                   e3: GR[Option[java.util.UUID]]): GR[SchemasRow] = GR { prs =>
    import prs._
    SchemasRow.tupled(
      (<<[String], <<?[String], <<[String], <<[java.util.UUID], <<?[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table schemas. Objects of this class serve as prototypes for rows in queries. */
  class Schemas(_tableTag: Tag) extends profile.api.Table[SchemasRow](_tableTag, "schemas") {
    def * = (name, description, psl, catalogId, backendConfigId, schemaId) <> (SchemasRow.tupled, SchemasRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(name), description, Rep.Some(psl), Rep.Some(catalogId), backendConfigId, Rep.Some(schemaId)).shaped.<>(
        { r =>
          import r._; _1.map(_ => SchemasRow.tupled((_1.get, _2, _3.get, _4.get, _5, _6.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column psl SqlType(varchar) */
    val psl: Rep[String] = column[String]("psl")

    /** Database column catalog_id SqlType(uuid) */
    val catalogId: Rep[java.util.UUID] = column[java.util.UUID]("catalog_id")

    /** Database column backend_config_id SqlType(uuid), Default(None) */
    val backendConfigId: Rep[Option[java.util.UUID]] =
      column[Option[java.util.UUID]]("backend_config_id", O.Default(None))

    /** Database column schema_id SqlType(uuid), PrimaryKey */
    val schemaId: Rep[java.util.UUID] = column[java.util.UUID]("schema_id", O.PrimaryKey)

    /** Foreign key referencing BackendConfigs (database name schemas_backend_config_id_fkey) */
    lazy val backendConfigsFk = foreignKey("schemas_backend_config_id_fkey", backendConfigId, BackendConfigs)(
      r => Rep.Some(r.backendConfigId),
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Catalogs (database name schemas_catalog_id_fkey) */
    lazy val catalogsFk = foreignKey("schemas_catalog_id_fkey", catalogId, Catalogs)(
      r => r.catalogId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table Schemas */
  lazy val Schemas = new TableQuery(tag => new Schemas(tag))

  /** Entity class storing rows of table SchemaUsages
    *  @param parentId Database column parent_id SqlType(uuid)
    *  @param childId Database column child_id SqlType(uuid) */
  case class SchemaUsagesRow(parentId: java.util.UUID, childId: java.util.UUID)

  /** GetResult implicit for fetching SchemaUsagesRow objects using plain SQL queries */
  implicit def GetResultSchemaUsagesRow(implicit e0: GR[java.util.UUID]): GR[SchemaUsagesRow] = GR { prs =>
    import prs._
    SchemaUsagesRow.tupled((<<[java.util.UUID], <<[java.util.UUID]))
  }

  /** Table description of table schema_usages. Objects of this class serve as prototypes for rows in queries. */
  class SchemaUsages(_tableTag: Tag) extends profile.api.Table[SchemaUsagesRow](_tableTag, "schema_usages") {
    def * = (parentId, childId) <> (SchemaUsagesRow.tupled, SchemaUsagesRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(parentId), Rep.Some(childId)).shaped.<>({ r =>
        import r._; _1.map(_ => SchemaUsagesRow.tupled((_1.get, _2.get)))
      }, (_: Any) => throw new Exception("Inserting into ? projection not supported."))

    /** Database column parent_id SqlType(uuid) */
    val parentId: Rep[java.util.UUID] = column[java.util.UUID]("parent_id")

    /** Database column child_id SqlType(uuid) */
    val childId: Rep[java.util.UUID] = column[java.util.UUID]("child_id")

    /** Foreign key referencing Schemas (database name schema_usages_child_id_fkey) */
    lazy val schemasFk1 = foreignKey("schema_usages_child_id_fkey", childId, Schemas)(
      r => r.schemaId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)

    /** Foreign key referencing Schemas (database name schema_usages_parent_id_fkey) */
    lazy val schemasFk2 = foreignKey("schema_usages_parent_id_fkey", parentId, Schemas)(
      r => r.schemaId,
      onUpdate = ForeignKeyAction.NoAction,
      onDelete = ForeignKeyAction.NoAction)
  }

  /** Collection-like TableQuery object for table SchemaUsages */
  lazy val SchemaUsages = new TableQuery(tag => new SchemaUsages(tag))

  /** Entity class storing rows of table Tbls
    *  @param tblId Database column tbl_id SqlType(serial), AutoInc, PrimaryKey
    *  @param dataSourceId Database column data_source_id SqlType(int4)
    *  @param name Database column name SqlType(varchar)
    *  @param displayName Database column display_name SqlType(varchar), Default(None)
    *  @param description Database column description SqlType(varchar), Default(None)
    *  @param params Database column params SqlType(varchar) */
  case class TblsRow(tblId: Int,
                     dataSourceId: Int,
                     name: String,
                     displayName: Option[String] = None,
                     description: Option[String] = None,
                     params: Map[String, String])

  /** GetResult implicit for fetching TblsRow objects using plain SQL queries */
  implicit def GetResultTblsRow(implicit e0: GR[Int],
                                e1: GR[String],
                                e2: GR[Option[String]],
                                e3: GR[Map[String, String]]): GR[TblsRow] = GR { prs =>
    import prs._
    TblsRow.tupled((<<[Int], <<[Int], <<[String], <<?[String], <<?[String], <<[Map[String, String]]))
  }

  /** Table description of table tbls. Objects of this class serve as prototypes for rows in queries. */
  class Tbls(_tableTag: Tag) extends profile.api.Table[TblsRow](_tableTag, "tbls") {
    def * = (tblId, dataSourceId, name, displayName, description, params) <> (TblsRow.tupled, TblsRow.unapply)

    /** Maps whole row to an option. Useful for outer joins. */
    def ? =
      (Rep.Some(tblId), Rep.Some(dataSourceId), Rep.Some(name), displayName, description, Rep.Some(params)).shaped.<>(
        { r =>
          import r._; _1.map(_ => TblsRow.tupled((_1.get, _2.get, _3.get, _4, _5, _6.get)))
        },
        (_: Any) => throw new Exception("Inserting into ? projection not supported.")
      )

    /** Database column tbl_id SqlType(serial), AutoInc, PrimaryKey */
    val tblId: Rep[Int] = column[Int]("tbl_id", O.AutoInc, O.PrimaryKey)

    /** Database column data_source_id SqlType(int4) */
    val dataSourceId: Rep[Int] = column[Int]("data_source_id")

    /** Database column name SqlType(varchar) */
    val name: Rep[String] = column[String]("name")

    /** Database column display_name SqlType(varchar), Default(None) */
    val displayName: Rep[Option[String]] = column[Option[String]]("display_name", O.Default(None))

    /** Database column description SqlType(varchar), Default(None) */
    val description: Rep[Option[String]] = column[Option[String]]("description", O.Default(None))

    /** Database column params SqlType(varchar) */
    val params: Rep[Map[String, String]] = column[Map[String, String]]("params")
  }

  /** Collection-like TableQuery object for table Tbls */
  lazy val Tbls = new TableQuery(tag => new Tbls(tag))
}
