# Concepts ..

### Schema

A schema defines which data sources are used and the relationships between them.
It is defined by [PSL](pslModelling.md), queries are always executed against a single schema.

Schema definitions include measures & dimensions, other included schemas,
and [conformance](pslReference.md) definitions between dimensions.

### Measure

A column (or combination via an expression) that can be aggregated 
(sum, average, count) and return a numeric value.
For example a datasource may contain an orderTotal column which can be defined
as a measure with a sum aggregation. 

### Dimension

A dimension is a group of dimension attributes. For example a User dimension may
contain the attributes: name, email, birthdate.

### Dimension Attribute

A dimension attributes can be added to a query to break-down a measure.
For example an orderTotal measure can be broken-down by the birthdate dimension attribute.


### Catalog

A catalog is a collection of schemas and datasources.
Schemas can only use datasources and include other schemas within the same catalog.

### DataSource

A DataSource contains connection details to a physical database or dataset.
