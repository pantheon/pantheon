package pantheon.backend.calcite.support.dialect;

import org.apache.calcite.sql.*;

public class Utils {

    private Utils() {
    }

    /**
     * If there is no TRUNC function in the DB, simulate this using calls to 'dateFormatFunc'.
     *
     * @param dateFormatFunc String date format function name having the following arguments: (dateColumn, formatString)
     * @param format         Date format that dateFormatFunc understands
     * @param suffix         Date part that will be concatenated with the formatted date
     * @param writer         Writer
     * @param call           Call
     *
     */
    public static void dateFormatFloor(String dateFormatFunc, String format, String suffix, SqlWriter writer, SqlCall call) {
        writer.print("CONCAT");
        SqlWriter.Frame concatFrame = writer.startList("(", ")");
        writer.print(dateFormatFunc);
        SqlWriter.Frame charFrame = writer.startList("(", ")");
        call.operand(0).unparse(writer, 0, 0);
        writer.sep(",", true);
        writer.print("'" + format + "'");
        writer.endList(charFrame);
        writer.sep(",", true);
        writer.print("'" + suffix + "'");
        writer.endList(concatFrame);
    }

}
