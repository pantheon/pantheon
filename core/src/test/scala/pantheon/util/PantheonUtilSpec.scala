package pantheon.util

import java.io.{IOException, InputStreamReader}
import org.scalatest.{MustMatchers, WordSpec}

class PantheonUtilSpec extends WordSpec with MustMatchers {

  "withResource" must {
    "close the resource after completion" in {
      val is = new InputStreamReader(System.in)
      noException should be thrownBy is.ready()
      withResource(is)(_ => ())
      the[IOException] thrownBy is.ready() must have message "Stream closed"
    }
  }

}
