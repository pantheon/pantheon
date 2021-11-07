package codegen

import slick.codegen.SourceCodeGenerator
import slick.jdbc.JdbcProfile
import slick.ast.ColumnOption

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

// Override the Slick SourceCodeGenerator.
// This uses some of the wrapping logic from
// https://github.com/slick/slick/blob/master/slick-codegen/src/main/scala/slick/codegen/SourceCodeGenerator.scala
// to construct a db connection and call SourceCodeGenerator with the right arguments.
object CustomGenerator {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case profile :: jdbcDriver :: url :: outputDir :: pkg :: Nil =>
        run(profile, jdbcDriver, url, outputDir, pkg, None, None, true)
      case _ =>
        println("CustomGenerator args incorrect")
        System.exit(1)
    }
  }

  def run(profile: String,
          jdbcDriver: String,
          url: String,
          outputDir: String,
          pkg: String,
          user: Option[String],
          password: Option[String],
          ignoreInvalidDefaults: Boolean): Unit = {
    val profileInstance: JdbcProfile = Class
      .forName(profile + "$")
      .getField("MODULE$")
      .get(null)
      .asInstanceOf[JdbcProfile]
    val dbFactory = profileInstance.api.Database
    val db = dbFactory.forURL(url,
                              driver = jdbcDriver,
                              user = user.getOrElse(null),
                              password = password.getOrElse(null),
                              keepAliveConnection = true)
    try {
      val m = Await.result(db.run(
                             profileInstance
                               .createModel(None, ignoreInvalidDefaults)(ExecutionContext.global)
                               .withPinnedSession),
                           Duration.Inf)

      // This is where the actual overriding of the Slick generator occurs.
      new SourceCodeGenerator(m) {
        // implicit converted from Map -> String, uses json to do serde
        val mapToStringImplicitCode =
          """
            |import play.api.libs.json._
            |import java.time.Instant
            |import java.sql.Timestamp
            |import slick.jdbc.PositionedResult
            |import services.QueryHistoryRepo.{CompletionStatus, completionStatusFormat}
            |import enumeratum.{EnumEntry, Enum}
            |import scala.reflect.ClassTag
            |
            |def enumeratumColumnType[E <: EnumEntry: ClassTag](e: Enum[E]) =
            |  MappedColumnType.base[E, String](
            |    _.toString,
            |    e.withName
            |  )
            |def enumColumnType[E <: Enumeration](e: E) = MappedColumnType.base[e.Value, String] (
            |  _.toString,
            |  e.withName(_)
            |)
            |implicit val queryHistoryRecordTypeColumnType = enumeratumColumnType(management.QueryHistoryRecordType)
            |implicit val queryTypeColumnType = enumColumnType(pantheon.QueryType)
            |implicit val resourceTypeColumnType = enumeratumColumnType(services.Authorization.ResourceType)
            |implicit val actionTypeColumnType = enumColumnType(services.Authorization.ActionType)
            |implicit val mapColumnType = MappedColumnType.base[Map[String, String], String] (
            |  { m => Json.stringify(Json.toJson(m)) },
            |  { s => Json.parse(s).as[Map[String, String]] }
            |)
            |implicit val completionStatusColumnType = MappedColumnType.base[CompletionStatus, String] (
            |  { m => Json.stringify(Json.toJson(m)) },
            |  { s => Json.parse(s).as[CompletionStatus] }
            |)
            |implicit val instantColumnType = MappedColumnType.base[Instant, Timestamp](Timestamp.from(_), _.toInstant)
            |
            |// allow user code to use this as an implicit,
            |// implicit GR[Map[String, String]] cannot be found outside Tables
            |val mapImpl: slick.jdbc.GetResult[Map[String, String]] =
            |  (rs: PositionedResult) => Json.parse(rs.nextString).as[Map[String, String]]
            |
          """.stripMargin

        override def code = mapToStringImplicitCode + "\n" + super.code

        // moving Row classes out of trait to make play json codec derivation work
        override def packageCode(profile: String,
                                 pkg: String,
                                 container: String,
                                 parentType: Option[String]): String = {
          s"""
            |package ${pkg}
            |// AUTO-GENERATED Slick data model
            |object ${container} {
            | val profile: slick.jdbc.JdbcProfile = $profile
            |  import profile.api._
            |  ${indent(code)}
            |}
          """.stripMargin
        }

        override def Table = new Table(_) {

          override def EntityType = new EntityType {
            // override final modifier in 3.2.0 which breaks compilation
            override def caseClassFinal = false
          }

          override def Column = new Column(_) { column =>
            // jsonb dependency in slick-pg is huge, so serialize instead
            def queryHistoryColumn(name: String) =
              column.model.table.table == "query_history" && column.name == name
            override def rawType = {
              if (column.name == "params") "Map[String, String]"
              else if (column.name == "properties") "Map[String, String]"
              else if (queryHistoryColumn("completionStatus")) "CompletionStatus"
              else if (queryHistoryColumn("queryType")) "pantheon.QueryType.Value"
              else if (queryHistoryColumn("`type`")) "management.QueryHistoryRecordType"
              else if (column.model.table.table == "roles" && column.name == "resourceType")
                "services.Authorization.ResourceType"
              else if (column.model.table.table == "role_actions" && column.name == "actionType")
                "services.Authorization.ActionType.Value"
              else if (super.rawType == "java.sql.Timestamp") "Instant"
              else super.rawType
            }
          }
        }
      }.writeToFile(profile, outputDir, pkg)
    } finally db.close
  }
}
