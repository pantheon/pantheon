package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.parser.SqlParserPos;

public class BigQueryDialect extends SqlDialect {
    public static final SqlDialect DEFAULT =
            new BigQueryDialect(EMPTY_CONTEXT
                    .withIdentifierQuoteString("`")
                    .withNullCollation(NullCollation.LOW));


    public BigQueryDialect(SqlDialect.Context context) {
        super(context);
    }

    @Override public boolean supportsCharSet() {
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#string-type
        return false;
    }

    // why are those needed at all?
    @Override public boolean supportsNestedAggregations() {
        return false;
    }

    @Override public SqlNode getCastSpec(RelDataType type) {
        String castSpec;
        switch (type.getSqlTypeName()) {
            case VARCHAR:
                castSpec = "STRING";
                break;
            case TINYINT:
                castSpec = "INT64";
                break;
            case SMALLINT:
                castSpec = "INT64";
                break;
            case INTEGER:
                castSpec = "INT64";
                break;
            case BIGINT:
                castSpec = "INT64";
                break;
            case FLOAT:
                castSpec = "FLOAT64";
                break;
            case DOUBLE:
                castSpec = "FLOAT64";
                break;
            case BOOLEAN:
                castSpec = "BOOL";
                break;
            // WARNING: TIMESTAMP conversion does not work
            case TIMESTAMP:
                castSpec = "DATETIME";
                break;
            // WARNING: TIMESTAMP_WITH_LOCAL_TIME_ZONE conversion does not work
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                castSpec = "TIMESTAMP";
                break;
            default:
                return super.getCastSpec(type);
        }

        return new SqlDataTypeSpec(
            new SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
            null,
            SqlParserPos.ZERO);
    }


    @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec) {

        switch (call.getKind()) {
            case FLOOR:
                // dateTime floor must have 2 arguments
                if (call.operandCount() != 2) {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                }
                else{
                    /*
                        Big query has different floor functions for different temporal types.
                        There is no way to reliably detect type of a field by its name.
                        Solution: support only Timestamp fields (Timestamp with timezone)
                    */
                    SqlFloorFunction.unparseDatetimeFunction(writer, call, "TIMESTAMP_TRUNC", true);
                }
                break;

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    // copied from mysql dialect
    @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
                                             SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }
}

