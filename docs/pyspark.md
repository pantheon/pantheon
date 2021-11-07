# Access with PySpark

Pantheon can be integrated with PySpark so that data scientists can
make multi-dimensional queries that return dataframes.
The Pantheon Python client wraps Pantheon core to provide the query
functionality, and can communicate with a running Pantheon server to fetch
dataSource and schema information.

## Requirements

* Python 3.6
* See [Quick Start](quickStart) to create example dataset, Pantheon build environment and all main entities

## Installation

* Build Pantheon core library
```
sbt core/assembly
```

* Download and install Pantheon Python client library:
```
git clone https://github.com/contiamo/pantheon-client.git
cd pantheon-client
make requirements install
```

* Download and extract [Spark](https://spark.apache.org/downloads.html)

* Download [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download.html)

## Using Pantheon client library inside pyspark environment

Run `pyspark` specifying PostgreSQL JDBC driver path and pantheon core library as following:
```
./bin/pyspark --driver-class-path postgresql-9.4-1206-jdbc41.jar --jars pantheon-core-assembly-0.2.1.jar 
```

To get some entities from Pantheon server:
```
import pantheon
client = pantheon.client.Client(('', ''))
```

To list catalogs:
```
client.catalogs.list()
```

To get particular catalog by catalog ID (assuming that catalog ID is 100):
```
catalog = client.catalogs.find(100)
```

To list schemas for the catalog:
```
catalog.schemas.list()
```

To get particular schema by name:
```
schema = catalog.schemas.find('Sales')
```

## Running queries 

Create query object:
```
query = pantheon.queries.Query(
    fields=['Store.region.country'],
    filter=[]
)
```

To run query on Pantheon server and return results:
```
schema.queries.execute(query)
```

To materialise the same query as a Spark dataframe:
```
# initialise Pantheon-PySpark integration
pp = pantheon.pyspark.Pyspark(spark, catalog)

# create dataframe out of the query
df = pp.execute_query(query, 'Sales')
```

Created dataframe incapsulates plan to fulfil the query inside this PySpark session. Note that dataframe is a *lazy* entity. Actual data retrieval is not processed until required.

This dataframe can be used as any other dataframe in PySpark:
```
# To display query plan for this dataframe
df.explain()

# To display dataframe rows
df.show()
```

