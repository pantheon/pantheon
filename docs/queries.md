#Queries

There are 3 types of query possible in Pantheon:

1. Aggregate: measures grouped by dimensions
2. Record: fetches unaggregated rows for given fields
3. SQL: standard SQL, using tables defined in a schema

Queries always target a specific Schema, so any fields in the query must reference
those in the Schema. Queries can also be persisted with optional parameters as
[savedQueries](savedQuery).

All examples used here are based on the Foodmart Schema example (see 
[installing test data](quickStart/#Installing%20test%20data))


### Aggregate Query

An aggregate query contains measures, dimensions, and filters.
For the most basic queries this is equivalent to the sql query: `SELECT a, sum(b) FROM t GROUP BY a`.

Property|Description
--------|-----------
type|"Aggregate"
rows|list of dimension attributes which form row headers
columns|list of dimension attributes which form column headers
measures|list of measures that go into resulting table body 
filter|applied before aggregation, can only use dimensions (for syntax see: [PSL Filter](psl/#filter))
aggregateFilter|filter to be applied after aggregation, can use dimensions & measures (for syntax see: [PSL Filter](psl/#filter))
orderBy| list of fields to sort by together with order direction
limit| integer limiting result set rows
offset| integer number of rows to skip in the result set
rowsTopN| section that defines limits on row values inside group
columnsTopN| section that defines limits on column values inside group

**Example:** Return aggregated sales by store attributes ordered by store name.

```testQuery
{
  "type": "Aggregate",
  "rows": ["Store.region.country", "Store.store.name"],
  "measures": ["sales"],
  "filter": "Date.year > 2017",
  "orderBy": [{ "name": "Store.store.name", "order": "Asc" }],
  "limit": 15
}
```

**Example:** Top 5 product brands by sales for each month.

```testQuery
{
  "type": "Aggregate",
  "rows": ["Date.month", "Product.brand"],
  "measures": ["sales"],
  "topNRows": {
    "orderBy": "sales",
    "dimensions": ["Date.month"],
    "n": 5
  }
}
```

**Example:** Sales by city with each year as a column.

```testQuery
{
  "type": "Aggregate",
  "rows": ["Store.region.city"],
  "columns": ["Date.year"],
  "measures": ["sales"],
  "limit": 10
}
```

### Record Query

A record query is a dimension-only query which does not perform any aggregation on the rows.
This is useful when there is a need to fetch individual records (e.g. last 10 events, or user name and email for user id = 7).

Property|Description
--------|-----------
type|"Record"
rows| list of dimension attributes to fetch, measures are not allowed here
filter|filter to apply to dimensions (for syntax see: [PSL Filter](psl/#filter))
orderBy| list of fields to sort by
limit| integer limiting result set rows
offset| integer number of rows to skip in the result set

**Example:** Retrieve customer details

```testQuery
{
  "type": "Record",
  "rows": ["Customer.id", "Customer.name"],
  "filter": "Customer.name like('Bob%')",
  "sort": [{ "name": "Customer.name", "order": "Asc" }],
  "limit": 100
}
```

### SQL Query

A SQL query can use any tables or columns defined in the pantheon schema.

The syntax is standard SQL: <https://calcite.apache.org/docs/reference.html>

Property|Description
--------|-----------
type|"Sql"
sql| SQL query text

**Example:** Retrieve customer details

```testQuery
{
  "type": "Sql",
  "sql": "select fname, lname from customer"
}
```

### Filter

Filter syntax is similar to SQL. String values must be single quoted (just like in SQL).

```
dim1 comparator val1 [boolOp dim2 comparator val2 ...]
```

Property|Description
--------|-----------
comparator|one of `=`, `!=`, `<`, `>`, `<=`, `>=`, `in`, `notIn`, `like`
boolOp|one of `and`

Example: 

```sql
"Store.Country = 'Germany' and Store.City = 'Berlin'"
```

