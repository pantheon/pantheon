package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDialect</code> implementation for the Oracle database.
 *
 * <p>Tested on Oracle XE 18.4.0.
 *
 * <p>Compatibility differences between Calcite's version of Oracle Dialect:
 *
 * <ul>
 *   <li>ROW_NUMBER() OVER clause does not support window bounds, so we trim them;
 *   <li>FLOOR() for DATE type is fixed for DAY and WEEK units to use proper specifiers;
 *   <li>Use DOUBLE PRECISION type instead of DOUBLE;
 * </ul>
 */
public class OracleDialect extends OracleSqlDialect {
  public static final SqlDialect DEFAULT =
      new OracleDialect(
          EMPTY_CONTEXT
              .withDatabaseProduct(DatabaseProduct.ORACLE)
              .withIdentifierQuoteString("\""));

  /** Creates a OracleDialect. */
  public OracleDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
      case OVER:
        // https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/ROW_NUMBER.html#GUID-D5A157F8-0F53-45BD-BF8C-AE79B1DB8C41
        //
        // ROW_NUMBER() does not support window bounds.
        switch (call.operand(0).getKind()) {
          case ROW_NUMBER:
            SqlWindow window = call.operand(1);
            window.setLowerBound(null);
            window.setUpperBound(null);
        }

        break;

      case FLOOR:
        // We rewrite FLOOR with 2 args into TRUNC for date type.
        if (call.operandCount() == 2) {
          if (unparseDateFloor(writer, call)) {
            return;
          }
        }

        break;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }

  private boolean unparseDateFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    String spec;

    // https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/ROUND-and-TRUNC-Date-Functions.html#GUID-8E10AB76-21DA-490F-A389-023B648DDEF8
    switch (unit) {
      case WEEK:
        // Same day of the week as the first day of the calendar week as
        // defined by the ISO 8601 standard, which is Monday.
        spec = "IW";
        break;

      case DAY:
        // 'DAY' is 'Starting day of the week', while 'J' is just 'Day'.
        spec = "J";
        break;

      case QUARTER:
        spec = "Q";
        break;

      default:
        return false;
    }

    writer.print("TRUNC");
    SqlWriter.Frame truncFrame = writer.startList("(", ")");

    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);

    writer.print("'" + spec + "'");
    writer.endList(truncFrame);

    return true;
  }

  @Override
  public SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case DOUBLE:
        // According to org.apache.calcite.sql.SqlDataTypeSpec we need to start
        // spec name from '_' to avoid quoting.
        castSpec = "_DOUBLE PRECISION";
        break;
      default:
        return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
        null,
        SqlParserPos.ZERO);
  }
}
