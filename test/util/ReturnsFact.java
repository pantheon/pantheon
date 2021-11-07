package util;

import java.sql.Date;

public class ReturnsFact {
    public final String sale_transaction_id;
    public final Date date;
    public final int hour;
    public final String reason;

    public ReturnsFact(String sale_transaction_id, Date date, int hour, String reason) {
        this.sale_transaction_id = sale_transaction_id;
        this.date = date;
        this.hour = hour;
        this.reason = reason;
    }
}
