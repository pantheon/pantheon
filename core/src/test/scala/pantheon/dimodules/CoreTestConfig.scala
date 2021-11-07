package pantheon.dimodules

import com.typesafe.config.ConfigFactory

trait CoreTestConfig {
  private val fileName = "test.conf"
  val config = ConfigFactory.load(fileName)
  assert(config.isResolved, s"cannot find test config file $fileName")
}
