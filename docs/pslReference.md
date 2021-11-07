# PSL reference 

## Syntax

The main entities ([dimensions](#dimension) and [measures](#measure)) describe the logical model of the schema. 
[Tables](#table) describe how logical entities maps to physical tables in datasources.

A simple example:

```testSchema
schema MySchema(dataSource = "my_postgres_db") {
  measure unitSales(column = "sales.unit_sales")

  dimension Customer(table = "customer") {
    attribute email(column = "email")
  }

  table sales {
    column customer_id(tableRef = "customer.customer_id")
    column unit_sales
  }
}
```

Now it's possible to query this schema for `unitSales` aggregated by a customer's `email`.

Generally PSL definitions have the following structure:

```
<definition> := <type> <name> <parameters>? <body>?
<parameters> := '(' <param> (',' <param>)* ')'
<param> := <paramName> '=' <paramValue>
<body> := '{' <definition>* '}'
```

## Schema

Top level element defining complete data model. 

### Syntax

`'schema' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
dataSource |Default data source for this schema. Data sources are referred by name inside the catalog containing this schema.

### Body

Contains the following definitions:

* Zero or more [Schema Imports](#schema-importexport)
* Zero or more [Schema Exports](#schema-importexport)
* Zero or more [Dimensions](#dimension)
* Zero or more [Measures](#measure)
* Zero or more [Tables](#table)
* At most one [Filter](#filter)

### Examples

```
schema MySchema(dataSource = "pg_prod") {
  dimension Time
}
```

## Schema Import/Export

The following entities can be imported/exported: schemas, dimensions and measures.
Import definition imports names inside current schema but does not expose them in schema interface. Imported definitions can be used, for example, in [conforms](#conforms) or other places where a reference to existing definition is necessary.
Export definition both imports names inside current schema as import does and at the same time exposes imported definitions in the schema interface.

Each imported name can have an alias. If an alias is not provided the entity will be imported with last name component in the qualified name (i.e. unqualified name).

### Syntax

```
'import' <refs>
'export' <refs>

Where:
<refs> := <qualifiedName> | (<qualifiedName>'.'<aliasedRefs>
<qualifiedName> := IDENTIFIER ('.' IDENTIFIER)*
<aliasedRefs> := '{' <aliasedRef> (',' <aliasedRef>)* '}'
<aliasedRef> := <name> ('=>' <alias>)?
<name> := IDENTIFIER
<alias> := IDENTIFIER
```

### Examples

```testSchema
/* 1. Importing */
schema Foodmart {
  import Sales                  // importing schema
  import {Inventory => inv}     // importing schema with an alias

  dimension Store {
    conforms Sales.Store        // using imported name
    conforms inv.Store
  }

  measure sales {
    conforms Sales.sales        // using imported name
  }
}
```

```testSchema
/* 2. Exporting */
schema Foodmart {
  export Sales   // exporting schema
  export {Inventory => Inv}

  /*
   * Exported names Store, sales, InvStore and invSales are accessible directly in queries to 
   * the Foodmart schema or when importing/exporting Foodmart schema itself.
   */

  dimension ConformingStore {
    conforms Store              // exported names can be used inside this schema as well
    conforms InvStore
  }
}
```

## Dimension

Defines related set of hierarchies, levels and attributes. Dimensions can accommodate different structures from multiple hierarchies of levels to simple plain set of attributes. Plain set of attributes just represents data in non-hierarchical format when all attributes are considered at the same level.

Dimension defines a namespace for the set of elements it contains and the name of the dimension is always the part of qualified level/attribute name.

When defining multiple hierarchies inside dimension one of them can be "default" by omitting name. In this case levels/attributes inside such hierarchy are addressed by omitting hierarchy name from fully qualified level/attribute name.

### Syntax

`'dimension' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
table |Default [table](#table) for this dimension.

### Body

Contains the following definitions:

* At most one [Metadata](#metadata)
* Zero or more [Conforms](#conforms)
* Either: 
  * One or more [Hierarchies](#hierarchy)
  * One or more [Levels](#level) 
  * One or more [Attributes](#attribute) 

### Examples

```
/* 
 * 1. Two different hierarchies 
 * First hierarchy here is the default.
 * Levels has the following qualified names:
 * Customer.ageGroup, Customer.age, Customer.customer, 
 * Customer.Geo.country, Customer.Geo.city, Customer.Geo.customer
 */
dimension Customer {
  hierarchy {
    level ageGroup
    level age
    level customer
  }
  
  hierarchy Geo {
    level country
    level city
    level customer
  }
}

/*
 * 2. Plain dimension structure 
 * In this case there are no hierarchical relationships inside the dimension
 * Attributes has the following qualified names: 
 * Person.id, Person.firstName, Person.lastName
 */
dimension Person {
  attribute id
  attribute firstName
  attribute lastName
}
```

## Hierarchy

Defines particular (optionally named) hierarchy of levels. The order of levels inside hierarchy defines hierarchical relationships from general to specific. Levels defined before are more broad than levels defined after. Pantheon uses this information to determine the direction of aggregation and to use aggregated tables attached to different levels.

### Syntax

`'hierarchy' <name>? <parameters>? <body>`

Note that name is optional. In such case this is the default hierarchy inside dimension. Only one hierarchy inside dimension allowed to have empty name.

### Parameters

Name |Description
---|---
table |Default [table](#table) for this hierarchy.
  
### Body

Contains the following definitions:

* At most one [Metadata](#metadata)
* Zero or more [Levels](#level) 

### Examples

```
hierarchy {
  level Country
  level Region
  level City
}
```

## Level

Defines level in hierarchy. Levels as well as attributes represent specific data values. It means that unlike dimensions and hierarchies which cannot be queried directly but provide just model structuring levels and attributes can be used in Pantheon queries.

In terms of modelling main difference between level and attribute is that level is used to model hierarchy while attribute doesn't participate in hierarchical relationships.

### Syntax

`'level' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
table |[Table](#table) for this level.
column |The name of the column for this level. If column name is not provided, level name is used instead.
expression | ??? Seems not used by now
castType | Specifies SQL type to convert output value for this level to.

### Body

Contains the following definitions:

* At most one [Metadata](#metadata)
* Zero or more [Conforms](#conforms)
* Zero or more [Attributes](#attribute) 

### Examples

```
level Country(column = "country_id") {
  attribute name
  attribute isoCountryCode
}
```

## Attribute

Defines data attribute that can be queried using with Pantheon.

### Syntax

`'attribute' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
table |[table](#table) for this attribute.
column |The name of the column for this attribute. If column name is not provided, attribute name is used instead.
expression | ??? Seems not used by now
castType | Specifies SQL type to convert output value for this attribute to.

### Body

Contains the following definitions:

* At most one [Metadata](#metadata)
* Zero or more [Conforms](#conforms)

### Examples

```
attribute Email(column = "user_email")
```

## Measure

Measure is an aggregatable piece of data. 

Measures can be defined in 3 different flavours:

* aggregated measure - measure that optionally apply filter to dimensions and then aggregate underlying column directly
* filtered measure - measure that references other measure and apply filtering on dimensions afterwards
* calculated measure - measure that implements some arithmetic expression on top of other measures

### Syntax

`'measure' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
aggregate |Aggregate function to use for this measure. Supported functions are: 'sum', 'avg', 'count', 'distinctCount', 'approxDistinctCount', 'max', 'min'. By default it is 'sum'.
measure |Reference to other measure to base this value on. Used in filtered measures.
filter |Filter to be applied when calculating this measure.
calculation |Arithmetic expression on other measures. Used in calculated measures.

The rules to determine measure type is the following:

* If neither `measure` nor `calculation` are defined that it is aggregated measure. Aggregation function determined either from `aggregate` parameter or "sum" by default.
* If `measure` is defined than it is filtered measure. In this case neither `calculation` nor `conforms` should be defined.
* If `calculation` is defined that it is calculated measure. In this case `aggregate`, `measure`, `filter` and `conforms` are prohibited.

### Body

Can contain the following definitions:

* At most one [Metadata](#metadata)
* Zero or more [Conforms](#conforms)

### Examples

```
measure unitSales(aggregate="sum")                // aggregated measure

measure unitSalesx(measure = "unitSales", filter = "Customer.cars < 3")  // filtered measure

measure numChildren(filter = "Customer.cars < 3")                           // aggregated measure with filter

measure calc (calculation = "storeSales + 100000 / (storeCost + storeSales) + 1") // calculated measure
```

## Table

Defines logical table based on physical table and provides mapping of columns to dimensions and measures. Table definition introduces new logical table name which is then used in tableRefs and dimensions. By default table name is the same as the physical table name. But for one physical table there can be multiple logical table definitions.

Tables are either defined in schema explicitly using this element or implicitly expected to exists in schema default data source when referenced in dimension hierarchies or other tables.

Table element can also define a view by using `sql` parameter.

### Syntax

`'table' <name> <parameters>? <body>?`

### Parameters

Name |Description
---|---
dataSource |The name of the data source for this table. Can reference datasources from this schema's catalog.
physicalTable |The name of underlying physical table for this logical table definition. Logical table name in used by default if this parameter is not provided.
sql |Defines an SQL statement instead of specific physical table. This should be written in backend's specific SQL dialect (Calcite or Spark).

Either `physicalTable` or `sql` parameter shall be defined. Not both.

### Body

Can contain the following definitions:

* Zero or more [Columns](#column)

### Examples

```
// 1. Table with body. Physical table name is also "sales_fact_1998"
table sales_fact_1998 { 
  column customer_id (tableRef = "customer.customer_id")
  column unit_sales
}

// 2. Using SQL view to define logical table
table viewTable(sql = """select * from "foodmart"."sales_fact_1998" sf8 join "foodmart"."time_by_day" td on (sf8.time_id = td.time_id)""") 
```

## Column

Column defines elementary piece of data inside logical table. This definition is also used to link physical data structure with logical elements such as dimensions and measures. As well as to define relations between logical tables.

### Syntax

`'column' <name> <parameters>?`

### Parameters

Name |Description
---|---
expression |SQL expression for this column. Expression shall be written in backend's SQL dialect in terms of enclosing physical table columns.
tableRef |Set of links to other table's columns. Table columns shall be addressed using `<table>.<column>` notation. The value for this parameter can be either single string or an array of strings. This attribute defines joining relations between tables.

### Examples

```
table sales_fact_1998 { 

  // Provides link to another table/column which can be used to join target table when necessary
  column customer_id (tableRef = "customer.customer_id")
  
  // tells that this column contains value for measure unitSales
  column unit_sales
}

table calendar {
  // An example of using expression
  column day_of_week(expression = """extract(day from "the_date")""")
}

table customer {
  // column can represent both dimension level/attribute and measure (count in this case)
  column customer_id
  
  // defining multiple dimension references
  column name
}
```

## Filter

Filter expressions have their own sql-like syntax.
A schema can have at most one filter.

### Syntax

```
filter "<expression> (<boolOp> <expression>)*"
<expression> := <dimAttr> <operator> <value>
<boolOp> := and | or
```

### Expression

Name  | Description
---|---
dimAttr | refers to a dimension attribute to filter by
operator| one of:  `=`, `!=`, `<`, `>`, `<=`, `>=`, `in`, `notIn`, `like`
value   | A dimension value (or values) to match

### Examples

```
filter "Date.Date > '2017-01-01'"

filter """
  Store.region.city in ('Tacoma', 'Seattle') and Store.store.type = 'Supermarket'
"""
```

## Conforms

Used to identify that multiple measures, dimensions, levels or attributes are the same across tables / datasources. Note that *conforms* relation is directed. That is if some element defined as conforming to some other element (has `conforms` definition in it's body) query for this element can return results found by means of element it conforms to. But not the other way round.

### Syntax

`'conforms' <qualifiedName>`

### Examples

```
schema Foodmart {
  import Sales.Store
  import Inventory.{Store => InvStore}
  import Sales.sales
  import Inventory.{warehouseSales => invSales}

  dimension ConformingStore {
    // defines conformance to 2 dimensions from other schemas
    // ConformingStore dimension will have intersection of hierarchies of dimension it conforms to
    conforms Store
    conforms InvStore
  }

  measure totalSales {
    // defines conformance to measure from other schema
    conforms sales
  }

  measure warehouseSales {
    // defines conformance to measure from other schema
    conforms invSales
  }
}
```

```
schema Foodmart {
  import {GoogleAnalytics => ga, FacebookStats => fb}

  // In this case we explicitly define set of attributes for dimension
  // for each attribute if there are such attribute (by name) in referred dimension it will be used as attribute to conform to
  dimension Date {
    conforms ga.Time
    conforms fb.Time

    attribute year
    attribute day
    attribute weekday
    attribute date
  }
}
```

## Metadata

Defines metadata for different types of elements. Metadata is not directly processed by Pantheon but stored for reference and for use in other tools. Metadata can be introspected to extract additional configuration or knowledge about the schema.

### Syntax

```
'metadata' <parameters>

Where:
<parameters> := '(' <param> (',' <param>)* ')'
<param> := <name> '=' (<simpleValue> | <arrayValue>)
<arrayValue> := '[' <simpleValue> (',' <simpleValue>)* ']'
<simpleValue> := BOOL | NUMBER | STRING
```

Parameter names here are arbitrary. They can be interpreted by third party tools.

### Examples

```
schema Foodmart {
  dimension D {
    metadata(d1 = ["d1", "dd1"], d2 = "d2")
    hierarchy H {
      metadata(h1 = ["h1", "hh1"], h2 = "h2")
      level L {
        metadata(l1 = ["l1", "ll1"], l2 = "l2", l3 = true)
        conforms l2
        attribute A {
          metadata(a1 = ["a1", "aa1"], a2 = "a2")
          conforms a2
        }
      }
    }
    hierarchy H2 {
      level L {
        metadata(l1 = ["l1", "ll1"], l2 = "l2")
      }
    }
  }
  measure M {
    metadata(m1 = ["m1", "mm1"], m2 = "m2")
  }
}
```
