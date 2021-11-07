package pantheon.util

import java.util.concurrent.Executors

import pantheon.schema.Compatibility

object Misc {
  val hikariHousekeeperThreadPool = Executors.newScheduledThreadPool(1)
}
