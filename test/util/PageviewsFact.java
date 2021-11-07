package util;

import java.util.Date;

public class PageviewsFact {
    public final Date date;
    public final int cust_id;
    public final String url;

    public PageviewsFact(Date date, int cust_id, String url) {
        this.date = date;
        this.cust_id = cust_id;
        this.url = url;
    }
}
