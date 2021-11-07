import com.simplytyped.Antlr4Plugin.autoImport.{antlr4GenListener, antlr4PackageName}
import com.typesafe.config.ConfigFactory
import sbt.addArtifact
import sbt.Keys.libraryDependencies

parallelExecution in ThisBuild := false

lazy val scala212 = "2.12.10"

lazy val codegen = project
  .settings(sharedSettings)
  // use >= 3.2 to support uuids
  .settings(
    Seq(
      name := "pantheon-codegen",
      libraryDependencies += "com.typesafe.slick" %% "slick-codegen" % "3.2.1",
      skip in publish := true
    ))

lazy val `jdbc-driver` = project
  .settings(
    Seq(
      name := "pantheon-jdbc",
      crossPaths := false,
      autoScalaLibrary := false,
      libraryDependencies ++= List(
        "org.slf4j" % "slf4j-api" % "1.7.25",
        "org.apache.calcite.avatica" % "avatica-core" % "1.13.0"
      ),
      assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
      assemblyShadeRules in assembly := Seq(
        ShadeRule.rename("org.apache.**" -> "pantheon.@0").inAll,
        ShadeRule.rename("org.slf4j.**" -> "pantheon.@0").inAll,
        ShadeRule.rename("com.fasterxml.**" -> "pantheon.@0").inAll,
        ShadeRule.rename("com.google.**" -> "pantheon.@0").inAll
      ),
      artifact in (Compile, assembly) := {
        val art = (artifact in (Compile, assembly)).value
        art.withClassifier(Some("assembly"))
      }
    )
  )
  .settings(addArtifact(artifact in (Compile, assembly), assembly).settings)

lazy val core = project
  .enablePlugins(Antlr4Plugin)
  .settings(sharedSettings)
  .settings(Seq(
    name := "pantheon-core",
    antlr4Version in Antlr4 := "4.7",
    antlr4PackageName in Antlr4 := Some("pantheon.schema.parser.grammar"),
    antlr4GenListener in Antlr4 := false,
    antlr4GenVisitor in Antlr4 := false,
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided" excludeAll (
        ExclusionRule("*", "log4j"),
        ExclusionRule("*", "slf4j-log4j12")
      ),
      "org.hsqldb" % "hsqldb" % "2.3.1" % Test
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    }
  ))
  .settings(addArtifact(artifact in (Compile, assembly), assembly).settings)

lazy val root = (project in file("."))
  .enablePlugins(PlayScala)
  .enablePlugins(SwaggerPlugin)
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion))
  .settings(sharedSettings)
  .settings(Seq(
    name := "pantheon-app",
    slick := slickCodeGenTask.value,
    skip in publish := true,
    routesImport ++= List(
      "java.util.UUID",
      "services.Authorization.ResourceType",
      "controllers.bindables._"
    ),
    libraryDependencies ++= List(
      ehcache,
      ws,
      filters,
      "net.debasishg" %% "redisclient" % "3.6",
      "com.typesafe.play" %% "play-slick" % "3.0.3",
      "com.typesafe.play" %% "play-slick-evolutions" % "3.0.3",
      "com.pauldijou" %% "jwt-play" % "0.16.0",
      "com.google.guava" % "guava" % "20.0",
      "org.apache.calcite.avatica" % "avatica-core" % "1.13.0",
      "org.apache.calcite.avatica" % "avatica-server" % "1.13.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0" excludeAll (
        ExclusionRule("*", "log4j"),
        ExclusionRule("*", "slf4j-log4j12")
      ),
// test dependencies
      "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.1" % Test
    ),
    swaggerV3 := true,
    swaggerDomainNameSpaces := Seq("java.util", "services", "controllers", "pantheon")
  ))
  // run code generator on compile
//  .settings(sourceGenerators in Compile += slickCodeGenTask.taskValue)
  .aggregate(core, `jdbc-driver`)
  .dependsOn(codegen)
  .dependsOn(core % "compile->compile;test->test")
  .dependsOn(`jdbc-driver` % "compile->compile;test->test")

lazy val sharedSettings = Seq(
  organization := "org.pantheon",
  version := "0.1.0-SNAPSHOT",

  resolvers := Seq(
      Resolver.mavenLocal
  ) ++ resolvers.value,

  scalaVersion := scala212,
  scalacOptions := Seq("-feature",
                       "-unchecked",
                       "-Ypartial-unification",
                       "-Xexperimental"),
  libraryDependencies ++= List(
    // test dependencies
    "org.scalatest" %% "scalatest" % "3.0.4" % Test,
    "org.mockito" % "mockito-all" % "1.10.19" % Test,
    // pantheon dependencies
    "com.zaxxer" % "HikariCP" % "2.7.9",
    "org.apache.calcite" % "calcite-core" % "1.22.0",
    "org.apache.calcite" % "calcite-mongodb" % "1.22.0",
    "org.apache.calcite" % "calcite-druid" % "1.14.0",
    "org.typelevel" %% "cats-core" % "1.1.0",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "com.typesafe" % "config" % "1.3.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.10.0",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0",
    "org.julienrf" %% "play-json-derived-codecs" % "4.0.1",
    "io.sentry" % "sentry-logback" % "1.6.3",
    "org.antlr" % "antlr4-runtime" % "4.7.1",
    "com.beachape" %% "enumeratum" % "1.5.12",
    "org.reflections" % "reflections" % "0.9.11",
    "org.postgresql" % "postgresql" % "9.4-1206-jdbc41",
    "io.jaegertracing" % "jaeger-client" % "0.33.1"
  ),
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case "META-INF/git.properties" | "checkstyle-cachefile" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

// get db config from application config
val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

// code generation task
lazy val slick = TaskKey[Seq[File]]("gen-tables")

lazy val slickCodeGenTask = Def.task {
  val cp = (dependencyClasspath in Compile).value
  val r = (runner in Compile).value
  val s = streams.value

  val url = conf.getString("slick.dbs.default.db.url")
  val jdbcDriver = conf.getString("slick.dbs.default.db.driver")
  val slickDriver = conf.getString("slick.dbs.default.profile").dropRight(1)

  val outputDir = "app"
  val pkg = "dao"
  r.run("codegen.CustomGenerator", cp.files, Array(slickDriver, jdbcDriver, url, outputDir, pkg), s.log)
    .failed foreach (sys error _.getMessage)
  val fname = outputDir + "/Tables.scala"
  Seq(file(fname))
}

enablePlugins(DockerPlugin)
val dockerTag = scala.util.Properties.envOrElse("DOCKER_TAG", "latest")
val jmxAgentURL = scala.util.Properties.envOrElse(
  "JMX_AGENT_URL",
  "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.3.1/jmx_prometheus_javaagent-0.3.1.jar")

import com.typesafe.sbt.packager.docker.Cmd
import com.typesafe.sbt.packager.docker.ExecCmd

// Default dockerCommands are overriden with custom steps in order to
// enforce a specific base image as well as conserve layer space by using
// the chown flag in the COPY command

// Once our Jenkins pipeline supports docker v17.06+/buildkit replace the two separate
// COPY/chmod steps with this single line:
// Cmd("COPY", "--chown=daemon opt /opt"),

dockerCommands := Seq(
  Cmd("FROM", "openjdk:11-jre-slim"),
  Cmd("WORKDIR", "/opt/docker"),
  Cmd("RUN", "apt-get update && apt-get -y install curl && apt-get clean"),
  Cmd("COPY", "opt /opt"),
  Cmd("RUN", "chown -R daemon /opt"),
  Cmd("USER", "daemon"),
  Cmd("RUN", "curl " + jmxAgentURL + " > conf/jmx_prometheus_javaagent.jar"),
  ExecCmd("ENTRYPOINT", "bin/pantheon-app", "-Dhttp.port=4300"),
  Cmd("CMD", "")
)

dockerAlias := DockerAlias(none, none, "pantheon", Some(dockerTag))
