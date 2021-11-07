package util;

import java.math.BigDecimal;
import java.sql.Date;

public class SalesFact {
  public final int cust_id;
  public final String sale_transaction_id;
  public final Date date;
  public final int hour;
  public final String payment_method;
  public final int address_id;
  public final BigDecimal amount;
  public final int num_items;

  public SalesFact(int cust_id, String sale_transaction_id, Date date, int hour,
    String payment_method, int address_id, BigDecimal amount, int num_items) {
      this.cust_id = cust_id;
      this.sale_transaction_id = sale_transaction_id;
      this.date = date;
      this.hour = hour;
      this.payment_method = payment_method;
      this.address_id = address_id;
      this.amount = amount;
      this.num_items = num_items;
  }
}
