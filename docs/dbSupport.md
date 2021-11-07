# MySQL 5.x

Note: MySQL 5.x is compatible with MariaDB JDBC driver

* Cast to `DOUBLE` is not supported (inserted by Calcite sometimes when expressions leads to floating point result)
* `FULL JOIN` is not supported (cannot combine multiple fact tables)
* GROUP BY column expression in output list sometimes cannot be inferred. Gives error:
```
Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 't1.birthDay' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by
```
* Window functions are not supported (cannot use TopN)

# MariaDB 10.x

* `FULL JOIN` is not supported (cannot combine multiple fact tables)
* Window functions are not supported (cannot use TopN)

# MySQL 8.x

Note: MySQL 8.x is not compatible with MariaDB JDBC driver

* Cast to `DOUBLE` is not supported (it's inserted by Calcite sometimes when expressions leads to floating point result)
* `FULL JOIN` is not supported (cannot combine multiple fact tables)

# MongoDB

Calcite MongoDB connector exposes MongoDB collections as a separate table each with single column `_MAP`. To convert such representation to relational view column contain expression like `cast(_MAP['customer_id'] AS integer)`.

* Joins are not pushed to Mongo but performed at Enumerable layer.