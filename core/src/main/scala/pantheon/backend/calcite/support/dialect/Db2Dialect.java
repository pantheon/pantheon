package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.fun.SqlFloorFunction;

public class Db2Dialect extends Db2SqlDialect {

    public static final SqlDialect DEFAULT =
            new Db2Dialect(EMPTY_CONTEXT
                    .withDatabaseProduct(DatabaseProduct.DB2)
            .withIdentifierQuoteString("\""));

    public Db2Dialect(SqlDialect.Context context) {
        super(context);
    }

    // if this is set to false - column names in outer project of a query sometimes get prepenended with non-existent table name (e.g: t2).
    @Override public boolean hasImplicitTableAlias() {
        return true;
    }

    @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case OVER:
                // WARNING: this code is copied from HanaDiealect. Window functions on DB2 have the same default behavior (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
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
                    throw new RuntimeException("DB2 supports only default window function bounds");
                }

            case FLOOR:
                // dateTime floor must have 2 arguments
                if (call.operandCount() != 2) {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                }
                else{
                    final SqlLiteral timeUnitNode = call.operand(1);
                    // TODO: TRUNK to 'DAY' returns unexpected (wrong) results on DB2. Need to investigate. Replacing with workaround.
                    if(timeUnitNode.getValue() == TimeUnitRange.DAY){
                         Utils.dateFormatFloor("TO_DATE","YYYY-MM-DD","",  writer, call);
                    }
                    else {
                        SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnitNode.getValue().toString(),
                                timeUnitNode.getParserPosition());
                        SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
                    }
                }
                break;

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

}

