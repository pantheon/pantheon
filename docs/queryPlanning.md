# Query Planning

PSL schema defines multidimensional model of available data. Queries are expressed in terms of this 
multidimensional model. For example, user can query some measures and dimensions and Pantheon figures out 
how to get data from underlying datasources aggregating and joining data when necessary.

To run multidimensional query Pantheon first translates it to relational model (the model implementing 
relational algebla, common for relational databases). Then it executes translated relational query 
using one of the available backends (Spark, Calcite, etc.)

Internally both multidimensional and relational queries are represented by query plans. So multidimensional 
query plan is translated to relational query plan by a series of steps and then passed to 
backend for further processing. Backends are responsible for translating common relational model as 
defined by Pantheon to query that is understood by particular technology used in the backend. 
For example, Spark backend translates relational query plan by applying API operations on dataframes.

## Query plan definitions

Query plan is a tree of nodes each inheriting from `QueryPlan`. All these nodes are defined in `QueryPlan.scala`.
There are some numeric and boolean expression definitions (base trait `Expression`), nodes using schema model and 
nodes using relational model.

Schema model nodes include:
* `SchemaProject`
* `SchemaAggregate`
* `UnresolvedSchema`
* `ResolvedSchema`
* `SuitableTables`
* `SuperJoin`

They express basic operations in PSL terms (measures, dimension attributes, schema, etc.)

Relational nodes include:
* `Project`
* `Aggregate`
* `Filter`
* `Sort`
* `Join`
* `View`

They express basic operations in relational terms (tables, columns, expressions).

## Planner

When user issues a query Pantheon:
* Creates schema-based query plan directly from the query. This plan is just another representation of the query.
* Transforms the plan to relational query plan
* Passes relational query plan to backend to execute
* Backend translates generic relational query plan to query representation specific to the backend
* Query is executed in the environment native to chosen backend

Transformation of schema-based query plan to relational query plan is the essential part of Pantheon. It is done by
rule-based query planner (`Planner.scala`). Planner takes sequence of transformation rules and executes them sequentially 
to build resulting query plan. Each rule transforms full query plan to another query plan. In other words query plan 
undergoes multiple transformations each described by planner rule.

List of planner rules are configurable and can be customized depending on use case. For example, initial query plan 
can be transformed to completely different output model (not relational one) or some optimization rules can be 
added or removed.

Default set or rules is the following:
* `ResolveSchema`
* `ResolveOutputFields`
* `ResolveFields`
* `FindSuitableTables`
* `ChooseOptimalCombination`
* `ConvertFilter`
* `ConvertAggregate`
* `ConvertProject`
* `ConvertExpressionsToView`
* `ConvertSuperJoin`

They can be divided roughly into 3 groups:
* _Resolving_ rules
* Choosing tables to fulfil the query
* Schema-based to relational _conversion_ rules

_Resolving_ rules basically "compiles" the query. When Pantheon first build query plan from incoming query it does not
check that measures, dimensions mentioned in query really exist in schema. Also there are some inherent constraints that
should be fulfilled. This group of rules resolves all mentioned fields and propagates these fields to nodes where they 
are required.

Next group of rules traverses available tables and chooses optimal combination of tables to fulfil the query.

_Conversion_ rules convert from schema-based plan nodes to relational plan nodes. For example, measure and dimension
attribute references are converted to table column references. "SuperJoin" is converted to combination of relational 
joins. Also at this stage some operations are pushed closer to base tables (like filter, for example). So some 
optimizations related to relational model are applied at this stage as well.

In the result we should have query plan with relational nodes only that can be understood and translated by backend.
