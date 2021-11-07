package util;

public class DataSchema {
  public SalesFact[] sales;
  public ReturnsFact[] returns;
  public CustomerDimension[] customers;
  public PageviewsFact[] pageviews;
  public ProductDimension[] products;

  public DataSchema() {
    sales = new SalesFact[]{};
    returns = new ReturnsFact[]{};
    customers = new CustomerDimension[]{};
    pageviews = new PageviewsFact[]{};
    products = new ProductDimension[]{};
  }
}
