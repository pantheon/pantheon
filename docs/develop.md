# Developing Pantheon

## Setup db 
```
# psql as superuser
psql> create database pantheon_development owner <user_to_run_pantheon>;

# download pantheon
sh> git clone https://github.com/contiamo/pantheon.git
sh> cd pantheon
# alter `slick.dbs.default` in `conf/application.conf` to point to your Postgres server
...
sh> sbt run
```

## Running integration tests

The `PantheonSpec` integration tests run common queries against a common dataset
(Foodmart) across multiple databases. The database connection details can be configured in:
`core/src/test/resources/test.conf`

Once configured the tests can be run with `sbt test`.
The databases, if available, are assumed to contain the foodmart dataset.

## Building Calcite 

We maintain a fork of Calcite which Pantheon references if not all our PRs are in the current
release, otherwise Pantheon refers to a released version of Calcite.
When developing a feature / fix for Calcite:

* Create an issue on Calcite jira: <https://issues.apache.org/jira/projects/CALCITE/issues>
* Create a branch of Calcite (either the relevant Contiamo fork, or Calcite master) & make changes
* Test the changes locally with Pantheon
* Run all Calcite tests
* Create a PR 
* Once merged into Calcite master, merge into Contiamo fork `contiamo_production` branch.
* Update Pantheon Calcite dependency version
* Once new Calcite is released, switch Pantheon to new Calcite version (if possible)

### Testing local changes with Pantheon:

```bash
# clone 
git clone https://github.com/contiamo/calcite.git

# create branch & make changes ...

# run specific tests
mvn -Dtest=RelToSqlConverterTest#testFloor test

# build Calcite (without tests)
mvn -DskipTests install 

```

To use the built Calcite version locally with Pantheon, the resolvers in [build.sbt](../build.sbt)
must contain `Resolver.mavenLocal` first.

