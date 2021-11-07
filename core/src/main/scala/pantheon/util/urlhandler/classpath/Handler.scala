package pantheon.util.urlhandler.classpath

import java.net.{URL, URLConnection, URLStreamHandler}

/**
  * This handler handles "classpath:" protocol for loading files from resources
  * Note that the protocol is defined by base package for this class.
  */
class Handler extends URLStreamHandler {

  override def openConnection(url: URL): URLConnection = {
    val res = this.getClass.getClassLoader.getResource(url.getPath)
    if (res == null) throw new java.io.IOException(s"Resource ${url.getPath} not found")
    else res.openConnection()
  }

}
