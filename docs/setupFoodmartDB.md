# Setup local foodmart DB with catalog datasource and schema


## Installing test data

For testing queries we will load the Foodmart dataset for Postgres:

```
# psql as superuser
psql> create user foodmart password 'foodmart';
psql> create database foodmart owner foodmart;

sh> git clone https://github.com/OSBI/foodmart-data.git
sh> cd foodmart-data
sh> cd data; unzip DataScript.zip; cd ..
sh> sh FoodMartLoader.sh --db postgres
```

Foodmart schema definition is available at <https://github.com/julianhyde/foodmart-data-hsqldb>

## Creating a catalog

A Catalog is a management entity and defines a namespace for data sources, schemas saved queries, etc.
To create a catalog:

```
curl -X POST -H "Content-Type: application/json" -d '{"name":"FoodmartQuickStart"}' http://localhost:4300/catalogs 
```

Server responds with newly created entity serialized as JSON:

```
{"name":"FoodmartQuickStart","description":null,"backendConfigId":null,"id":"a77ebdc1-9dbc-46c9-a236-55d342ea3bd1"}
```

## Creating a DataSource

A DataSource is used to configure access details to the underlying database, filesytem, or remote endpoint. 
Data from multiple data sources can be combined in the same Schema (see below).

To create a datasource for foodmart schema in PostgreSQL (note that in the URL "a77ebdc1-9dbc-46c9-a236-55d342ea3bd1" is 
the identifier of the catalog created at the previous step):

```
curl -X POST -H "Content-Type: application/json" -d '{"name":"foodmart","type":"jdbc","locationUri":"jdbc:postgresql:foodmart","params":{"username":"foodmart","password":"foodmart"}}' http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/dataSources 
```

Server responds with newly created entity serialized as JSON:

```
{"name":"foodmart","dataSourceProductId":null,"description":null,"params":{"username":"foodmart","password":"foodmart"},"id":"5ecc48ef-f2a2-42f6-a36e-2aeb718a92fa","locationUri":"jdbc:postgresql:foodmart","catalogId":"a77ebdc1-9dbc-46c9-a236-55d342ea3bd1","type":"jdbc"}
```

## Creating a Schema

Pantheon schemas are used to define data models.

Individual data sources normally contain sets of tables. We are going to provide additional structure to these tables 
to define multi-dimensional data model. Normally multi-dimensional models are defined as *snowflake* or *star* schemas where we 
have one big *fact* table and a number of *dimensions* either included in this fact table or connected to it by using foreign keys. 
After multi-dimensional schema is defined we are able to query it for defined dimension attributes and measures and Pantheon will 
automatically process necessary joins, aggregations, etc. required to get specific values.

For example, our `foodmart` data source contains fact table `sales_fact_1998` with metrics `sales`, `cost` and `unitSales`. This 
fact table is linked to a few other tables representing dimension including:

* `time_by_day` table by `time_id` column
* `customer` table by `customer_id` column
* `store` table by `store_id` column

Table `store` further linked to table `region` by `region_id` column.

These and other relations between tables in the *Foodmart* schema can be seen in visual 
form at: <https://github.com/julianhyde/foodmart-data-hsqldb/blob/master/foodmart-schema.png>

We are going to define corresponding dimensions with hierarchies and measures based on these inherent relations.

In order to define schema create a file named `sales.psl` with the following contents:
```
schema Sales (dataSource = "foodmart") {

  dimension Date (table = "time_by_day") {
    level year (column = "the_year")
    level month (column = "the_month")
    level date (column = "the_date")
  }

  dimension Store(table = "store") {
    level region(column = "region_id", table = "region") {
      attribute city(column = "sales_city")
      attribute province(column = "sales_state_province")
      attribute region(column = "sales_region")
      attribute country(column = "sales_country")
    }
    level store(column = "store_id") {
      attribute name(column = "store_name")
      attribute type(column = "store_type")
    }
  }

  dimension Customer(table = "customer") {
    level country
    level province(column = "state_province")
    level city
    level customer(column = "customer_id") {
      attribute firstName(column = "fname")
      attribute lastName(column = "lname")
      attribute birthDate(column = "birthdate")
      attribute birthYear
      attribute gender
    }
  }

  measure sales(column = "sales_fact_1998.store_sales")
  measure cost(column = "sales_fact_1998.store_cost")
  measure unitSales(column = "sales_fact_1998.unit_sales")

  table customer {
    column birthYear(expression = "year(birthdate)")
  }

  table store {
    column region_id (tableRef = "region.region_id")
  }

  table sales_fact_1998 {
    column time_id (tableRef = "time_by_day.time_id")
    column customer_id (tableRef = "customer.customer_id")
    column store_id (tableRef = "store.store_id")

    column store_sales
    column store_cost
    column unit_sales
  }

}
```

Here we defined schema `Sales` based on `foodmart` datasource.

Inside schema we defined 3 dimensions:

* `Date` dimension with levels:
    * `year`
    * `month`
    * `date`
* `Store` dimension with levels:
    * `region`
    * `store`
* `Customer` dimension with levels:
    * `country`
    * `province`
    * `customer`
  
Some of the levels also define attributes inside them. All attributes defined at the same level have equal place in dimension 
hierarchy. That is they require the same level of measure aggregation. Level itself implicitly defines attribute with the same 
name. For example, both `Store.region` and `Store.region.city` can be queried. The former will return region ID and the latter 
will return city name.

Note that schema allows to define logical names for all entities and provide the mapping to physical tables/columns.

One dimension can span multiple tables. This is the case for the `Store` dimension. Some attributes are stored in base 
table `store` which is specified on the dimension itself. But attributes or the `region` level are stored in table `region`. 
Dimension does not specify how to combine these tables. This relation is defined below when defining columns in the `store` table.

Three measures `sales`, `cost` and `unitSales` are defined below dimensions. One essential attribute of the measure is the way 
to aggregate it because multi-dimensional queries automatically aggregate measures to the required granularity. By default, 
if `aggregate` parameter is not provided for a measure it will use `sum` aggregator.

Below measures there are 3 *logical* table definitions. Note that dimensions references these *logical* tables, not 
the *physical* ones existing in particular data sources. Logical table can be based directly on the physical one 
(optionally with different name) or be the product of some SQL statement.

Table columns are also flexible. They can be either physical columns existing in particular table. Or they can define some 
SQL expression to apply to the columns of the physical table. An example of such expression is the `birthYear` column 
definition in `customer` table.

To relate tables to each other there is `tableRef` definition. It is analogous to foreign key definitions in relational 
databases and tells that a column in one table references a column (normally primary key) in another. These table references 
are used both for relating fact tables to dimensions and also to relate dimensional tables.

To tell where measure values are stored we need to link column to measure by `measureRef` definition. For example, in our 
schema we are linking columns `store_sales`, `store_cost` and `unit_sales` from the table `sales_fact_1998` to corresponding 
measures. Note that defining tables and columns on measures themselves is not possible. This is done like that because 
generally there is no *master* table for a measure. Measures can (and should) be stored in multiple tables depending on 
necessary aggregation levels. While for dimensions there is normally one *master* table which contains all values for 
given dimension attributes.

In order to create the above schema run the following commands:

```
SCHEMA_ESCAPED=`cat sales.psl | sed 's#"#\\\\"#g' | tr '\n' ' '`
curl -X POST -H "Content-Type: application/json" -d "{\"psl\": \"$SCHEMA_ESCAPED\"}" http://localhost:4300/catalogs/a77ebdc1-9dbc-46c9-a236-55d342ea3bd1/schemas
```

Server shall respond with the newly created schema.

