package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.*;

public class HanaDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new HanaDialect(EMPTY_CONTEXT
          .withIdentifierQuoteString("\""));

  public HanaDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                    int leftPrec, int rightPrec) {

    switch (call.getKind()) {
    case OVER:
      SqlWindow window = call.operand(1);
      SqlNode lowerBound = window.getLowerBound();
      SqlNode upperBound = window.getUpperBound();

      if ((lowerBound != null) && (lowerBound.getKind() == SqlKind.LITERAL) &&
          ((SqlLiteral) lowerBound).getValue().toString().equals("UNBOUNDED PRECEDING") &&
          (upperBound != null) && (upperBound.getKind() == SqlKind.LITERAL) &&
          ((SqlLiteral) upperBound).getValue().toString().equals("CURRENT ROW")) {
        window.setLowerBound(null);
        window.setUpperBound(null);
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;

      } else {
        throw new RuntimeException("Hana supports only default window function bounds");
      }

    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      unparseDateFloor(writer, call);
      return;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * There is no TRUNC function in Hana so simulate this using calls to DATE_FORMAT.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseDateFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    String format;
    String suffix;
    switch (unit) {
    case YEAR:
      format = "YYYY";
      suffix = "-01-01";
      break;
    case MONTH:
      format = "YYYY-MM";
      suffix = "-01";
      break;
    case DAY:
      format = "YYYY-MM-DD";
      suffix = "";
      break;
    case HOUR:
      format = "YYYY-MM-DD HH24";
      suffix = "";
      break;
    case MINUTE:
      format = "YYYY-MM-DD HH24:MM";
      suffix = "";
      break;
    case SECOND:
      format = "YYYY-MM-DD HH24:MM:SS";
      suffix = "";
      break;
    default:
      // WEEK, among others, is not supported - there is an ISOWEEK function which returns data in
      // the format '2019-W19', but there's no function to turn that into a proper date
      throw new RuntimeException("Hana does not support FLOOR for time unit: " + unit);
    }

    Utils.dateFormatFloor("TO_NVARCHAR", format, suffix, writer, call);
  }
}

