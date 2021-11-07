package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.TeradataSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDialect</code> implementation for the Teradata database.
 *
 * <p>Tested on Teradata 15.
 *
 * <p>Compatibility differences between Calcite's version of Teradata Dialect:
 *
 * <ul>
 *   <li>ROW_NUMBER() OVER clause does not support window bounds, so we trim them;
 *   <li>FLOOR() for DATE type syntax is rewritten in form TRUNC('value', 'spec');
 *   <li>Use DOUBLE PRECISION type instead of DOUBLE;
 *   <li>CAST() doesn't support conversion between character sets, so we trim it;
 * </ul>
 */
public class TeradataDialect extends TeradataSqlDialect {
  public static final SqlDialect DEFAULT =
      new TeradataDialect(
          EMPTY_CONTEXT
              .withDatabaseProduct(DatabaseProduct.TERADATA)
              .withIdentifierQuoteString("\""));

  /** Creates a TeradataDialect. */
  public TeradataDialect(SqlDialect.Context context) {
    super(context);
  }
  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
      case OVER:
        // https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8AEiTSe3nkHWox93XxcLrg
        //
        // ROW_NUMBER() does not support window bounds.
        switch (call.operand(0).getKind()) {
          case ROW_NUMBER:
            SqlWindow window = call.operand(1);
            window.setLowerBound(null);
            window.setUpperBound(null);
        }

        break;

      case CAST:
        // https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/he7n6dg_ru2dYFTI5oh~Vg
        //
        // *** Failure 5355 The arguments of the CAST function must be of the
        //     same character data type.
        //
        // a) It _seems_ that Teradata's CAST() doesn't support conversion from one
        //    character set to another, so right now we just trim any character
        //    set conversion.
        // b) Teradata allows only small subset of character sets which
        //    should be specified as SQL identifiers (like UNICODE), and should
        //    not be 'quoted' (like "UTF-16LE").
        //
        // TODO use TRANSLATE() call if conversion between encodings should be required.
        SqlDataTypeSpec spec = call.operand(1);
        SqlTypeNameSpec typenameSpec = spec.getTypeNameSpec();
        if (typenameSpec instanceof SqlBasicTypeNameSpec
            && ((SqlBasicTypeNameSpec) typenameSpec).getCharSetName() != "") {
          call.setOperand(1, new SqlDataTypeSpec(typenameSpec, null, SqlParserPos.ZERO));
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

    // https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/5rM7GF6kTb0Mvnp4CcK50g
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

      case MONTH:
        spec = "MONTH";
        break;

      case QUARTER:
        spec = "Q";
        break;

      case YEAR:
        spec = "Y";
        break;

      default:
        throw new RuntimeException("Teradata does not support FLOOR for time unit: " + unit);
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
