package pantheon.util

import java.net.{URL, URLStreamHandler, URLStreamHandlerFactory}
import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, SQLFeatureNotSupportedException}
import java.util.Properties

import pantheon.util.urlhandler.classpath.Handler

object DriverRegistry {

  val ResourceUrlProto = "classpath"
  val HandlerPkgsProperty = "java.protocol.handler.pkgs"
  val UrlHandlerPackage = "pantheon.util.urlhandler"

  {
    URL.setURLStreamHandlerFactory(protocol => {
      if (protocol == ResourceUrlProto) new Handler
      else null
    })
  }

  /**
    * DriverManager requires driver to be loaded with the same classloader as applet or application.
    * So we wrap driver loaded by custom classloader with wrapper loaded by application classloader
    * @param driver
    */
  class DriverWrapper(val driver: Driver) extends Driver {
    override def acceptsURL(url: String): Boolean = driver.acceptsURL(url)

    override def jdbcCompliant(): Boolean = driver.jdbcCompliant()

    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
      driver.getPropertyInfo(url, info)

    override def getMinorVersion: Int = driver.getMinorVersion

    def getParentLogger: java.util.logging.Logger = throw new SQLFeatureNotSupportedException(s"Not supported.")

    override def connect(url: String, info: Properties): Connection = driver.connect(url, info)

    override def getMajorVersion: Int = driver.getMajorVersion
  }

  @volatile
  private var drivers = Map.empty[(String, Option[String]), Driver]

  def get(driverClassName: String, driverClasspath: Option[String]): Driver = {

    def parseClasspath(cp: String): Array[URL] = {
      cp.split(';').map(new URL(_))
    }

    def constructDriver(c: Class[_]): Driver = c.getDeclaredConstructor().newInstance().asInstanceOf[Driver]

    // Discussion note(to be deleted): .map brings runtime overhead and generally not used for function composition(used andThen instead)
    val loadDriver = (parseClasspath _)
      .andThen(new ReverseURLClassLoader(_, getClass.getClassLoader))
      .andThen(Class.forName(driverClassName, true, _))
      .andThen(constructDriver)
      .andThen(new DriverWrapper(_))

    drivers.getOrElse(
      (driverClassName, driverClasspath),
      synchronized {
        drivers.getOrElse(
          (driverClassName, driverClasspath), {

            val driver = driverClasspath
              .map(loadDriver)
              .getOrElse(constructDriver(Class.forName(driverClassName)))

            drivers = drivers + ((driverClassName, driverClasspath) -> driver)
            DriverManager.registerDriver(driver)
            driver
          }
        )
      }
    )
  }
}
