import java.nio.file.{Files, Paths}

import play.api.libs.json._
import play.api.test._
import play.api.test.Helpers._
import _root_.util.Fixtures
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import org.scalatestplus.play.BaseOneAppPerSuite
import pantheon.schema.parser.SchemaParser
import pantheon.schema.{FoodmartSchemaFixture, SchemaAST, SchemaCompilationError}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DocExamplesIntegrationSpec extends Fixtures with BaseOneAppPerSuite with TableDrivenPropertyChecks {

  import scala.language.implicitConversions

  case class Block(fileLocation: String, name: String, lines: List[String])

  val blocks: List[Block] = {
    val blks = ListBuffer[Block]()

    val docsDir = Paths.get(s"docs")
    if (Files.isDirectory(docsDir)) {
      val files = Files.list(docsDir).iterator().asScala.filter(_.getFileName.toString.endsWith(".md"))
      files must not be empty

      files.foreach { f =>
        val fName = f.getFileName.toString
        val lines = Files.readAllLines(f).asScala.toList

        val curBlock = ListBuffer[String]()
        var inMatch = false
        var bName = ""
        var startLine = 0

        lines.zipWithIndex.map { case (l, i) => (l.trim, i) }.foreach {
          case (l, lineNum) =>
            if (inMatch) {
              if (l == "```") {
                // match ends
                blks += Block(s"$fName:$startLine", bName, curBlock.toList)
                curBlock.clear()
                inMatch = false
              } else {
                curBlock += l
              }
            } else if (l.startsWith("```test")) {
              bName = l.substring("```".length)
              inMatch = true
              startLine = lineNum + 1
            }
        }
      }
    } else {
      fail("could not find docs directory")
    }

    blks.toList
  }

  "Query examples in docs" should {

    "should complete successfully" in {
      val fixture = new FoodmartSchemaFixture {}
      val (catId, schemaId) = createFoodMartBasedSchema("foodmart", fixture.fullFoodmart)

      val qBlocks = blocks.filter(_.name == "testQuery")

      forAll(new TableFor1("query block", qBlocks: _*)) { block =>
        val blockQuery = block.lines.mkString("\n")
        val queryBody = Json.parse(blockQuery)
        val query =
          route(app, FakeRequest(POST, s"/catalogs/$catId/schemas/$schemaId/query").withJsonBody(queryBody)).get

        status(query) mustBe OK
        contentType(query) mustBe Some("application/json")
      }

    }
  }

  def parseSchema(schema: String): Either[List[SchemaCompilationError], SchemaAST] =
    SchemaParser(schema).left.map(e => List(SchemaCompilationError(e)))

  "Schema examples in docs" should {

    "should parse successfully" in {
      val sBlocks = blocks.filter(_.name == "testSchema")

      forAll(new TableFor1("schema block", sBlocks: _*)) { block =>
        val blockSchema = block.lines.mkString("\n")

        val schema = parseSchema(blockSchema)
        schema mustBe 'right
      }

    }
  }
}
