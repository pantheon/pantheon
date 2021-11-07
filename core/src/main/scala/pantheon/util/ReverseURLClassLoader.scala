package pantheon.util

import java.net.{URL, URLClassLoader}
import java.util

import scala.collection.JavaConverters._

object ReverseURLClassLoader {
  class ParentClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = super.findClass(name)
    override def loadClass(name: String): Class[_] = super.loadClass(name)
    override def loadClass(name: String, resolve: Boolean): Class[_] = super.loadClass(name, resolve)
  }
}

/**
  * This classloader reverses normal classloading sequence where parent classloader tried first.
  * In order to do that we set parent to null and try to load classes with this classloader first.
  * If class cannot be loaded with this classloader then we fall back to the parent.
  * @param urls array or URLs for classpath resolution
  * @param parent parent classloader
  */
class ReverseURLClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, null) {

  import ReverseURLClassLoader.ParentClassLoader

  private val parentClassLoader = new ParentClassLoader(parent)

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    try {
      super.loadClass(name, resolve)
    } catch {
      case e: ClassNotFoundException =>
        parentClassLoader.loadClass(name, resolve)
    }
  }

  override def getResource(name: String): URL = {
    val url = super.findResource(name)
    val res = if (url != null) url else parentClassLoader.getResource(name)
    res
  }

  override def getResources(name: String): util.Enumeration[URL] = {
    val childUrls = super.findResources(name).asScala
    val parentUrls = parentClassLoader.getResources(name).asScala
    (childUrls ++ parentUrls).asJavaEnumeration
  }

  override def addURL(url: URL) {
    super.addURL(url)
  }

}
