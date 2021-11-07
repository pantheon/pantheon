package pantheon

import java.net.URI

import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3}
import org.scalatest.{MustMatchers, WordSpec}
import pantheon.DataSource.DriverClassLoc

class DataSourceSpec extends WordSpec with MustMatchers with TableDrivenPropertyChecks {

  "DataSource.create positive cases" must {

    val commonProps =
      Map("host" -> "hostX",
          "port" -> "99",
          "database" -> "dbX",
          "user" -> "X",
          "driverClassName" -> "driverName",
          "driverClassPath" -> "driverPath")

    "hsql datasource" in {
      val dsName = "hsqlDs"

      DataSource.create(dsName, "hsqldb-foodmart", commonProps) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:hsqldb:res:foodmart",
          Map("database" -> "dbX", "user" -> "FOODMART", "password" -> "FOODMART", "port" -> "99", "host" -> "hostX",  "schema" -> "foodmart"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }
    "postgres datasource" in {
      val dsName = "postgresqlDs"

      DataSource.create(dsName, "postgresql", commonProps) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:postgresql://hostX:99/dbX?currentSchema=public",
          Map("user" -> "X", "schema" -> "public"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "postgres datasource with non-default schema" in {
      val dsName = "postgresqlDs"

      DataSource.create(dsName, "postgresql", commonProps + ("schema" -> "schemaX")) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:postgresql://hostX:99/dbX?currentSchema=schemaX",
          Map("user" -> "X", "schema" -> "schemaX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "mysql datasource with default protocol" in {
      val dsName = "mysqlDs"
      DataSource.create(dsName, "mysql", commonProps) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:mysql://hostX:99/dbX",
          Map("user" -> "X", "schema" -> "dbX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "mysql datasource with non-default protocol" in {
      val dsName = "mysqlDs"
      DataSource.create(dsName, "mysql", commonProps + ("protocol" -> "loadbalance")) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:loadbalance://hostX:99/dbX",
          Map("user" -> "X", "schema" -> "dbX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )

      DataSource.create(dsName, "mysql", commonProps + ("protocol" -> "mysqlx")) mustBe Right(
        JdbcDataSource(
          dsName,
          "mysqlx://hostX:99/dbX",
          Map("user" -> "X", "schema" -> "dbX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "clickhouse datasource" in {
      val dsName = "clickhouseDs"
      DataSource.create(dsName, "clickhouse", commonProps) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:clickhouse://hostX:99/dbX",
          Map("user" -> "X", "schema" -> "dbX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "clickhouse datasource with database detected from schema param" in {
      val dsName = "clickhouseDs"
      DataSource.create(dsName, "clickhouse", commonProps - "database" + ("schema" -> "dbX")) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:clickhouse://hostX:99/dbX",
          Map("user" -> "X", "schema" -> "dbX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }

    "hana datasource" in {
      val dsName = "hanaDS"
      DataSource.create(dsName, "hana", commonProps + ("schema" -> "schemaX")) mustBe Right(
        JdbcDataSource(
          dsName,
          "jdbc:sap://hostX:99/?databaseName=dbX&currentschema=schemaX",
          Map("user" -> "X", "schema" -> "schemaX"),
          Some(DriverClassLoc("driverName", Some("driverPath"))),
          None
        )
      )
    }
    "jdbc datasource (with schema detection)" in {
      val jdbcDsName = "jdbcDs"
      val classLoc = DriverClassLoc("classname", Some("class/path"))
      val jdbcParams = Map("url" -> "jdbc:postgresql://localhost/test", "currentSchema" -> "s1") ++ classLoc.toProps

      DataSource.create(jdbcDsName, "jdbc", jdbcParams) mustBe Right(
        JdbcDataSource(
          jdbcDsName,
          "jdbc:postgresql://localhost/test",
          Map("currentSchema" -> "s1"),
          Some(classLoc),
          None
        )
      )
    }

    "reflective DataSource" in {
      val reflectiveDsName = "reflectiveDs"
      val reflectiveTagret = "xxx"
      DataSource.create(reflectiveDsName, "reflective", Map("target" -> reflectiveTagret)) mustBe Right(
        ReflectiveDataSource(reflectiveDsName, reflectiveTagret)
      )
    }

    "druid datasource" in {
      val druidDsName = "druidDs"
      val druidCoordUri = "xxx"
      val druidUri = "http://localhost:3000"
      //test without opt params
      // coordinatorUrl is parsed as URI for some reason
      val druidParams1 = Map("url" -> druidUri,
                             "coordinatorUrl" -> druidCoordUri,
                             "approxDecimal" -> "true",
                             "approxTopN" -> "true",
                             "approxDistinctCount" -> "true")
      val druidParams2 = Map("url" -> druidUri,
                             "coordinatorUrl" -> druidCoordUri)

      DataSource.create(druidDsName, "druid", druidParams1) mustBe Right(
        DruidSource(druidDsName, new URI(druidUri), new URI(druidCoordUri), true, true, true)
      )

      DataSource.create(druidDsName, "druid", druidParams2) mustBe Right(
        DruidSource(druidDsName, new URI(druidUri), new URI(druidCoordUri), false, false, false)
      )
    }
  }

  "DataSource.create negative cases" must {
    "jdbc datasource without url" in {
      DataSource.create("jdbcDs", "jdbc", Map()) mustBe Left(
        s"cannot build data source of type 'jdbc' from props, 'url' must be defined"
      )
    }

    "reflective DataSource with location uri or without target" in {
      DataSource.create("reflectiveDs", "reflective", Map()) mustBe Left(
        "cannot build data source of type 'reflective' from props, 'target' must be defined"
      )
    }

    "druid datasource without location uri, coordinator uri, or malformed boolean params" in {
      DataSource.create("druidDs", "druid", Map()) mustBe Left(
        s"cannot build data source of type 'druid' from props, 'url' must be defined"
      )

      DataSource.create("druidDs", "druid", Map("url" -> "xxx")) mustBe Left(
        "cannot build data source of type 'druid' from props, 'coordinatorUrl' must be defined"
      )

      forAll(Table("bool param name", "approxDecimal", "approxDistinctCount", "approxTopN"))(param =>
        DataSource
          .create("druidDs", "druid", Map("url" -> "xxx", "coordinatorUrl" -> "xxx", param -> "foo")) mustBe Left(
          s"cannot build data source of type 'druid' from props, cannot parse boolean from foo"
      ))
    }
  }

}
