package pantheon.backend.calcite

import java.sql.ResultSet

import org.mockito.Mockito.when
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatest.mockito.MockitoSugar.mock
import pantheon.backend.calcite.JdbcRowSet.JdbcRowIterator

class JdbcRowSetSpec extends WordSpec with MustMatchers {

  "JdbcRowIterator" must {
    "move the cursor before the first invocation" in {
      val resultSet = mock[ResultSet]
      var cursorMoved = false

      when(resultSet.next()).`then` { _ =>
        cursorMoved = true; true
      }
      val it = new JdbcRowIterator(resultSet)
      it.next()
      it.resultSet mustEqual resultSet
      cursorMoved mustBe true
    }

    "cursor must not be moved on multiple subsequent calls to hasNext or when next is called after hasNext" in {
      val resultSet = mock[ResultSet]
      when(resultSet.next()).thenReturn(true).thenThrow(new RuntimeException("calling next twice!!"))
      val ri = new JdbcRowIterator(resultSet)

      ri.hasNext() mustBe true
      ri.hasNext() mustBe true
      ri.hasNext() mustBe true
      ri.next()
      ri.resultSet mustEqual resultSet
    }

    "NoSuchElementException should be thrown when next is called on empty ResultSet" in {
      val resultSet = mock[ResultSet]
      when(resultSet.next()).thenReturn(false).thenThrow(new RuntimeException("calling next twice!!"))
      val ri = new JdbcRowIterator(resultSet)

      the[NoSuchElementException] thrownBy ri.next() must have message "invoking next on empty Iterator[Row]"
      ri.hasNext() mustBe false
      ri.hasNext() mustBe false
      ri.hasNext() mustBe false
    }

  }

}
