package jdbc

import java.sql.DriverManager

import org.scalatestplus.play.BaseOneAppPerSuite
import org.scalatest.{Outcome}
import play.api.Application
import util.{Fixtures, ResultSetUtil}
import pantheon.util.withResource

import java.util.concurrent.TimeUnit
import config.{PantheonAppLoader, PantheonComponents}
import messagebus.SingleNodeMessageBus

class JdbcSpec extends Fixtures with BaseOneAppPerSuite {

  override def withFixture(test: NoArgTest): Outcome = {
    // initializing pantheon jdbc driver
    Class.forName("com.contiamo.pantheon.jdbc.Driver")
    super.withFixture(test)
  }

  override lazy val components = new PantheonComponents(appContext, (as, _, mp) => new SingleNodeMessageBus(mp, as.dispatcher)) {
    override val jdbcMeta: PantheonMetaImpl = {
      val connectionCacheConfig = Cache.Config(10, 100, 1000, 8, TimeUnit.SECONDS)
      val statementCacheConfig = Cache.Config(10, 100, 1000, 5, TimeUnit.SECONDS)
      new PantheonMetaImpl(backendConfigRepo, schemaRepo, connectionCacheConfig, statementCacheConfig)(executionContext)
    }
  }

  override def fakeApplication: Application = new PantheonAppLoader(_ => components, true).load(appContext)

  def withConnection(f: java.sql.Connection => Unit): Unit = {
    val (catalogId, schemaId) = provideSchema()
    val url = s"jdbc:pantheon://localhost:8765/$catalogId/Foodmart"

    withResource(DriverManager.getConnection(url, new java.util.Properties()))(f)
  }

  "JBDC" should {
    "SQL statement works" in {
      withConnection { conn =>
        val stmt = conn.createStatement()
        withResource(stmt.executeQuery("select count(*) from customer")) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """EXPR$0=10281
              |""".stripMargin
        }
        stmt.close()
      }
    }

    "SQL statement with schema prefix works" in {
      withConnection { conn =>
        val stmt = conn.createStatement()
        withResource(stmt.executeQuery("select count(*) from Foodmart.customer")) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """EXPR$0=10281
              |""".stripMargin
        }
        stmt.close()
      }
    }

    "SQL prepared statement works" in {
      withConnection { conn =>
        val stmt = conn.prepareStatement("select count(*) from customer")
        withResource(stmt.executeQuery()) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """EXPR$0=10281
              |""".stripMargin
        }
        stmt.close()
      }
    }

    "SQL large query works" in {
      withConnection { conn =>
        val stmt = conn.prepareStatement("select * from sales_fact_1998")
        withResource(stmt.executeQuery()) { rs =>
          ResultSetUtil.count(rs) mustBe 164558
        }
        stmt.close()
      }
    }

    "Reconnects on connection timeout" in {
      withConnection { conn =>
        Thread.sleep(10000)
        val stmt = conn.prepareStatement("select count(*) from customer")
        withResource(stmt.executeQuery()) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """EXPR$0=10281
              |""".stripMargin
        }
        stmt.close()
      }
    }

    "Reconnects in the middle of reading result" in {
      withConnection { conn =>
        val stmt = conn.prepareStatement("select * from sales_fact_1998")
        withResource(stmt.executeQuery()) { rs =>
          (1 to 100).foreach(_ => rs.next())
          Thread.sleep(10000)
          ResultSetUtil.count(rs) mustBe 164458
        }
        stmt.close()
      }
    }

    "switch schema for connection" in {
      withConnection { conn =>
        conn.setSchema("Foodmart")
        conn.getSchema mustBe "Foodmart"
      }
    }

    "get list of schemas" in {
      withConnection { conn =>
        withResource(conn.getMetaData.getSchemas) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """TABLE_SCHEM=Foodmart; TABLE_CATALOG=
              |""".stripMargin
        }
      }
    }

    "get list of table types" in {
      withConnection { conn =>
        withResource(conn.getMetaData.getTableTypes) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """TABLE_TYPE=TABLE
              |""".stripMargin
        }
      }
    }

    "get database properties" in {
      withConnection { conn =>
        conn.getMetaData.getSQLKeywords mustBe "ABS,ALLOW,ARRAY,ARRAY_MAX_CARDINALITY,ASENSITIVE,ASYMMETRIC,ATOMIC,BEGIN_FRAME,BEGIN_PARTITION,BIGINT,BINARY,BLOB,BOOLEAN,CALL,CALLED,CARDINALITY,CEIL,CEILING,CLASSIFIER,CLOB,COLLECT,CONDITION,CONTAINS,CORR,COVAR_POP,COVAR_SAMP,CUBE,CUME_DIST,CURRENT_CATALOG,CURRENT_DEFAULT_TRANSFORM_GROUP,CURRENT_PATH,CURRENT_ROLE,CURRENT_ROW,CURRENT_SCHEMA,CURRENT_TRANSFORM_GROUP_FOR_TYPE,CYCLE,DEFINE,DENSE_RANK,DEREF,DETERMINISTIC,DISALLOW,DYNAMIC,EACH,ELEMENT,EMPTY,END_FRAME,END_PARTITION,EQUALS,EVERY,EXP,EXPLAIN,EXTEND,FILTER,FIRST_VALUE,FLOOR,FRAME_ROW,FREE,FUNCTION,FUSION,GROUPING,GROUPS,HOLD,IMPORT,INITIAL,INOUT,INTERSECTION,JSON_ARRAY,JSON_ARRAYAGG,JSON_EXISTS,JSON_OBJECT,JSON_OBJECTAGG,JSON_QUERY,JSON_VALUE,LAG,LARGE,LAST_VALUE,LATERAL,LEAD,LIKE_REGEX,LIMIT,LN,LOCALTIME,LOCALTIMESTAMP,MATCHES,MATCH_NUMBER,MATCH_RECOGNIZE,MEASURES,MEMBER,MERGE,METHOD,MINUS,MOD,MODIFIES,MULTISET,NCLOB,NEW,NONE,NORMALIZE,NTH_VALUE,NTILE,OCCURRENCES_REGEX,OFFSET,OLD,OMIT,ONE,OUT,OVER,OVERLAY,PARAMETER,PARTITION,PATTERN,PER,PERCENT,PERCENTILE_CONT,PERCENTILE_DISC,PERCENT_RANK,PERIOD,PERMUTE,PORTION,POSITION_REGEX,POWER,PRECEDES,PREV,RANGE,RANK,READS,RECURSIVE,REF,REFERENCING,REGR_AVGX,REGR_AVGY,REGR_COUNT,REGR_INTERCEPT,REGR_R2,REGR_SLOPE,REGR_SXX,REGR_SXY,REGR_SYY,RELEASE,RESET,RESULT,RETURN,RETURNS,ROLLUP,ROW,ROW_NUMBER,RUNNING,SAVEPOINT,SCOPE,SEARCH,SEEK,SENSITIVE,SHOW,SIMILAR,SKIP,SPECIFIC,SPECIFICTYPE,SQLEXCEPTION,SQLWARNING,SQRT,START,STATIC,STDDEV_POP,STDDEV_SAMP,STREAM,SUBMULTISET,SUBSET,SUBSTRING_REGEX,SUCCEEDS,SYMMETRIC,SYSTEM,SYSTEM_TIME,TABLESAMPLE,TINYINT,TRANSLATE_REGEX,TREAT,TRIGGER,TRIM_ARRAY,TRUNCATE,UESCAPE,UNNEST,UPSERT,VALUE_OF,VARBINARY,VAR_POP,VAR_SAMP,VERSIONING,WIDTH_BUCKET,WINDOW,WITHIN,WITHOUT"
      }
    }

    "get type info" in {
      withConnection { conn =>
        withResource(conn.getMetaData.getTypeInfo) { rs =>
          ResultSetUtil.count(rs) mustBe 45
        }
      }
    }

    "get list of tables" in {
      withConnection { conn =>
        withResource(conn.getMetaData.getTables(null, null, null, null)) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=customer; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null
              |TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=store; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null
              |TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=sales_fact_1998; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null
              |""".stripMargin
        }
      }
    }

    "get list of columns" in {
      withConnection { conn =>
        withResource(conn.getMetaData.getColumns(null, null, "sales_fact_1998", null)) { rs =>
          ResultSetUtil.toString(rs) mustBe
            """TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=sales_fact_1998; COLUMN_NAME=customer_id; DATA_TYPE=4; TYPE_NAME=INTEGER; COLUMN_SIZE=10; BUFFER_LENGTH=null; DECIMAL_DIGITS=0; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=10; ORDINAL_POSITION=1; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=
              |TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=sales_fact_1998; COLUMN_NAME=store_id; DATA_TYPE=4; TYPE_NAME=INTEGER; COLUMN_SIZE=10; BUFFER_LENGTH=null; DECIMAL_DIGITS=0; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=10; ORDINAL_POSITION=2; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=
              |TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=sales_fact_1998; COLUMN_NAME=unit_sales; DATA_TYPE=3; TYPE_NAME=DECIMAL; COLUMN_SIZE=10; BUFFER_LENGTH=null; DECIMAL_DIGITS=4; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=10; ORDINAL_POSITION=3; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=
              |TABLE_CAT=; TABLE_SCHEM=Foodmart; TABLE_NAME=sales_fact_1998; COLUMN_NAME=store_sales; DATA_TYPE=3; TYPE_NAME=DECIMAL; COLUMN_SIZE=10; BUFFER_LENGTH=null; DECIMAL_DIGITS=4; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=10; ORDINAL_POSITION=4; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=
              |""".stripMargin
        }
      }
    }
  }

}
