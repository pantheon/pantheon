package util

import java.sql.ResultSet

object ResultSetUtil {
  def toString(resultSet: ResultSet): String = {
    val buf = new StringBuilder()
    while (resultSet.next()) {
      val n = resultSet.getMetaData.getColumnCount
      for (i <- 1 to n) {
        buf
          .append(if (i > 1) "; " else "")
          .append(resultSet.getMetaData.getColumnLabel(i))
          .append("=")
          .append(resultSet.getObject(i))
      }
      buf.append("\n")
    }
    buf.toString()
  }

  def count(resultSet: ResultSet): Int = {
    var i = 0
    while (resultSet.next()) {
      i += 1
    }
    i
  }
}
