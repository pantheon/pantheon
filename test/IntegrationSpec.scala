import org.scalatestplus.play._
import org.scalatestplus.play.components.OneServerPerSuiteWithComponents
import util.Fixtures

/**
  * add your integration spec here.
  * An integration test will fire up a whole play application in a real (or headless) browser
  */
class IntegrationSpec
    extends Fixtures
    with OneServerPerSuiteWithComponents
    with OneBrowserPerSuite
    with HtmlUnitFactory {

  override lazy val context = appContext

  "Application" should {

    "work from within a browser" in {

      go to ("http://localhost:" + port)

      pageSource must include("Your new application is ready.")
    }
  }
}
