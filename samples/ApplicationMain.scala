package com.contiamo

import java.sql.{DriverManager, ResultSet}

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContextExecutor
import akka.http.scaladsl.server.Directives._
import spray.json.DefaultJsonProtocol

trait Service extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer

  def config: Config
  val logger: LoggingAdapter

  val PREFIX = "prefix"

//  val routes = rejectEmptyResponse {
//    logRequestResult("pantheon") {
//      pathPrefix(PREFIX) {
//        get { req =>
//          complete(Backend.query)
//        }
//      }
//    }
//  }
}

object ApplicationMain extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  override val config = ConfigFactory.load
  override val logger = Logging(system, getClass)

//  val driver = "org.apache.derby.jdbc.EmbeddedDriver"
//  val dbName = "derbydb"
//  val connectionURL = "jdbc:derby:" + dbName
//
//  Class.forName(driver)
//  val conn = DriverManager.getConnection(connectionURL)

  def printres(resultSet: ResultSet): Unit = {
    val buf = new StringBuilder()
    while (resultSet.next()) {
      val n = resultSet.getMetaData.getColumnCount
      for (i <- 1 to n) {
        buf
          .append(if (i > 1) "; " else "")
          .append(resultSet.getMetaData.getColumnLabel(i))
          .append("=")
          .append(resultSet.getObject(i))
      }
      System.out.println(buf.toString())
      buf.setLength(0)
    }
    resultSet.close()
  }

//  val types = Array("TABLE", "VIEW")
//  printres(conn.getMetaData.getTables(null, "APP", null, types))
//  printres(conn.getMetaData.getCatalogs)
//  println("done")
  Backend.query
//  Http().bindAndHandle(
//      routes, config.getString("http.interface"), config.getInt("http.port"))
}
