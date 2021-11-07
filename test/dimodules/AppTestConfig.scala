package dimodules

import com.typesafe.config.ConfigFactory

trait AppTestConfig {
  //test application.conf is merged with main application.conf from the app. (Probably done by play plugin)
  val fileName = "application.conf"
  val config = ConfigFactory.load(fileName)
  assert(config.isResolved, s"cannot find test config file $fileName")
}