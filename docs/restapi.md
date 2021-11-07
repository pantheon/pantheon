# REST API

## Running queries via Rest API

To run queries there is RESTful endpoint which takes JSON query in the following format:

```
{
  "type": "Olap",
  "rows": ["Store.region.country", "Store.region.city"],
                                                 // (optional) list of dimension attributes to be displayed in rows 
  "columns": ["Customer.country"],               // (optional) list of dimension attributes to be displayed as column headers
  "measures": ["sales"],                         // (optional) list of measures to be displayed in the body of the table
  "filter": "Store.region.country = 'US'",       // (optional) filter
  "offset": 0,                                   // (optional) offset for resulting rows, default 0
  "limit": 100,                                  // (optional) limit for resulting rows
  "orderBy": [                                   // (optional) list of columns to order by
    {"name": "sales", "order": "desc"} 
  ],
  "rowsTopN": {                                  // (optional) Top N clause for rows
    "orderBy": [{"name": "Store.region.city", "order": "asc"}],
    "dimensions": ["Store.region.country"],
    "n": 5
  },
  "columnsTopN": {                               // (optional) Top N clause for columns
    ...                                          // same syntax as rowsTopN
  }
}
```

`type` attribute is the type of query. Currently it can be one of: "Olap", "Entity" or "Sql". Different query types have different set of allowed parameters.

`rows` and `columns` attributes contain the list of dimension attributes to return as row and column headers correspondingly. Dimension attributes are specified using qualified name including dimension name, hierarchy name (if hierarchy is named), level name and attribute name (if we need attribute inside level). Qualified name contains of all these components separated by dot.

`measures` attribute contains the list of measures to return in table body.

`filter` attribute specifies the filter to be applied to dimensions. Filter can use simple comparisons and logical operators (see [PSL Filter](psl/#filter) for more details).

### Dimension values

To get the values for one dimension attribute:

```
curl -X POST -H "Content-Type: application/json" -s -d '{"type": "Olap", "rows": ["Store.region.country"]}' http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/schemas/8d3304e6-9fc3-4fe7-a50f-d279fdc54f64/query | json_pp
```

where "a77ebdc1-9dbc-46c9-a236-55d342ea3bd1" is the catalog id and "8d3304e6-9fc3-4fe7-a50f-d279fdc54f64" is the schema id.

Pantheon shall return something like:

```
{
   "columns" : [
      {
         "key" : "Store.region.country",
         "kind" : "dimension",
         "metadata" : {},
         "primitive" : "string"
      }
   ],
   "rows" : [
      [
         "USA"
      ],
      [
         "Canada"
      ],
      [
         "No Country"
      ],
      [
         "Mexico"
      ]
   ]
}
```

There are 2 sections in the response: `columns` and `rows`.

`columns` section provides description for requested columns, including:

* `key` - field name as requested in the query
* `kind` - dimension (for dimension attributes) or measure
* `primitive` - JSON type of the value
* `metadata` - metadata, attached to the dimension attribute or measure

Columns in this section are enumerated in the same order they are enumerated in the query request.

`rows` section returns requested values in rows. Values in each row positionally correspond to columns defined in the `columns` section.

### Measure-only

To get measure value aggregated totally:

```
curl -X POST -H "Content-Type: application/json" -s -d '{"type": "Olap", "measures": ["sales"]}' http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/schemas/8d3304e6-9fc3-4fe7-a50f-d279fdc54f64/query | json_pp
```


### Full OLAP query

In this case we are requesting sales by store's country:

```
curl -X POST -H "Content-Type: application/json" -s -d '{"type": "Olap", "rows": ["Store.region.country"], "measures": ["sales"]}' http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/schemas/8d3304e6-9fc3-4fe7-a50f-d279fdc54f64/query | json_pp
```

The response will be similar to:

```
{
   "columns" : [
      {
         "metadata" : {},
         "key" : "Store.region.country",
         "primitive" : "string",
         "kind" : "dimension"
      },
      {
         "primitive" : "number",
         "kind" : "measure",
         "metadata" : {},
         "key" : "sales"
      }
   ],
   "rows" : [
      [
         "Mexico",
         430293.59
      ],
      [
         "USA",
         550808.42
      ],
      [
         "Canada",
         98045.46
      ]
   ]
}
```

### OLAP query with filtering and sorting

We want to return US cities ordered by sales amount:

```
curl -X POST -H "Content-Type: application/json" -s -d "{\"type\": \"Olap\", \"rows\": [\"Store.region.country\",\"Store.region.city\"], \"measures\": [\"sales\"],\"filter\":\"Store.region.country='USA'\",\"orderBy\":[{\"name\":\"sales\",\"order\":\"desc\"}]}" http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/schemas/8d3304e6-9fc3-4fe7-a50f-d279fdc54f64/query | json_pp
```

The response will be similar to:

```
{
   "columns" : [
      {
         "primitive" : "string",
         "metadata" : {},
         "kind" : "dimension",
         "key" : "Store.region.country"
      },
      {
         "primitive" : "string",
         "kind" : "dimension",
         "key" : "Store.region.city",
         "metadata" : {}
      },
      {
         "primitive" : "number",
         "metadata" : {},
         "kind" : "measure",
         "key" : "sales"
      }
   ],
   "rows" : [
      [
         "USA",
         "Tacoma",
         75219.13
      ],
      [
         "USA",
         "Salem",
         74965.24
      ],
      [
         "USA",
         "Seattle",
         56579.46
      ],
      [
         "USA",
         "Spokane",
         54984.94
      ],
      [
         "USA",
         "Portland",
         53633.26
      ],
      [
         "USA",
         "San Diego",
         51620.4
      ],
      [
         "USA",
         "Bremerton",
         51135.19
      ],
      [
         "USA",
         "Los Angeles",
         50819.15
      ],
      [
         "USA",
         "Beverly Hills",
         47843.92
      ],
      [
         "USA",
         "Yakima",
         20764.95
      ],
      [
         "USA",
         "Walla Walla",
         4847.95
      ],
      [
         "USA",
         "San Francisco",
         4230.02
      ],
      [
         "USA",
         "Bellingham",
         4164.81
      ]
   ]
}
```
