package jdbc

import org.apache.calcite.avatica._
import org.apache.calcite.avatica.remote.{LocalService, Service}
import org.apache.calcite.avatica.server.{AvaticaJsonHandler, AvaticaProtobufHandler, HttpServer}

object JdbcServer {

  object PantheonDriverVersion extends DriverVersion("", "", "", "", false, 1, 0, 1, 0)

  val JDBC_PORT = 8765

  private var server: Option[HttpServer] = None

  def start(meta: Meta): Unit = {
    val service = new LocalService(meta)
    val httpServer = new HttpServer(JDBC_PORT, new AvaticaProtobufHandler(service))
    httpServer.start()
    server = Some(httpServer)
  }

  def stop(): Unit = {
    server.foreach(_.stop())
    server = None
  }
}
