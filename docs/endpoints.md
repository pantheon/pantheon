# Saved Query & Endpoints

A saved query is a predefined query (of any [type](query)) with optional placeholders which can 
be provided at execution time.
A saved query can be used to limit access to a schema, and provide useful
default fields for simple clients.

### Creation & Editing

A saved query has the following basic attributes:

* catalogId
* schemaName
* base
* name 
* description

`base` attribute contains the same object used in query endpoint (it can contain any type of query, including "Aggregate", "Record" and "Sql").

Example:

```
// POST /catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/savedQueries
{
  "schemaName": "Order",
  "name": "orderInfo",
  "base": {
    "type": "Aggregate",
    "rows": ["Order.id", "Order.type"],
    "measures": ["orderTotal"],
    "filter": "Order.id = :orderId",
    "limit": 100,
    "orderBy": [ { "name": "Order.id", "order": "Ascending" } ]
  }
}
```

### Execution

When executing the saved query the following attributes can be supplied:

* params: list of values to fill in the placeholders

additional attributes when queryType is record or aggregate:

* fields: if empty, all fields in the saved query will be in the result
* filter: additional filters to be added

Example:

```
// POST /catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/savedQueries/5ecc48ef-f2a2-42f6-a36e-2aeb718a92fa/execute
{
  "fields": ["Order.id"],
  "params": [
    { "name":"orderId", "value": 45 }
  ],
  "filter": "orderTotal > 150.00",
  "limit": 1,
  "offset": 5,
  "orderBy": [ { "name": "Order.id", "order": "Ascending" } ]
}
```
