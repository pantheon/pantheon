Pantheon is the central access point for all your analytics data.

It provides three main functionalities:

* Data source registry
* Virtual (federated) SQL database
* Measure and dimension definitions and Aggregate query interface

## Prerequisites

- Java 8 JDK
- Install and start [Postgres](https://www.postgresql.org/download/).
- Create two databases called `pantheon_development` and `pantheon_test` (see below for user requirements)

## Launch Pantheon server

To start the Pantheon server:

```bash
# make sure local pantheon_development database access credentials match those in:
# conf/application.conf

# start application locally at port 4300
sbt "run 4300"

# start application in production mode
sbt start

# To launch in cluster mode (adds ability to cancel queries when multiple Pantheon instances are running)
# provide your own PantheonAppLoader implementation or use existing one in Application.conf
play.application.loader=config.ClusterAppLoaderRedis
```

## Seed database

To create initial data in empty database use `db_seed.sh`

## Evolving the database

Pantheon uses Play evolutions which are run automatically on start.

```
# update structure.sql (after running evolutions)
./bin/db_dump.sh

# generate code for data models (after running evolutions)
sbt genTables
```

## Running tests

Pantheon requires a local `pantheon_test` postgres database to run tests against.
Access credentials must match those in [/test/resources/application.conf](./test/resources/application.conf).

```
# run all tests
sbt test
```

To test specific Data Source check documentation:

* [Oracle](./docs/oracle.md);
* [Teradata](./docs/teradata.md);

## Documentation

Pantheon docs live in [/docs](./docs)

```
# build the docs for the first time
docker build -t pantheon/mkdocs-material:latest -f docs/mkdocs.Dockerfile .

# run the docs server locally
docker run --rm -it -p 8080:8080 -v `pwd`:/docs pantheon/mkdocs-material:latest serve --dev-addr=0.0.0.0:8080
```

The latest OpenApi v3 spec can be found at [/docs/swagger.json](./docs/swagger.json)

```
# regenerate swagger.json to target/swagger/
sbt swagger && ./bin/fixSwagger.sh
```

## Developing Pantheon

Code should be formatted using scalafmt:

```
# format scala code changed in git diff
sbt scalafmt --diff
# check code is formatted correctly
sbt scalafmt --test
```

### Git flow

* Work on a feature branch, update against master regularly
* Open a PR when it is ready for review, after this:
  * update using merge; not rebase (final changes will be squashed anyway)
  * this allows reviewers to track changes more easily
* Merge to `master` via squashing
  * commit message should use the PR title and number (as github suggests by default)
  * remove unnecessary details (commit messages) from the message body

Important for reviewers to check:

* Format of PR title - this will be used to autogenerate a changelog so should be:
  * concise yet descriptive
  * start with a capital letter
  * start with `FIX: ` if it's a bugfix, no prefix otherwise
* Is the branch up to date with `master`?

## Datascience integration

Pantheon core can be embedded in a notebook. For this a pantheon-core jar must be created. Pantheon core supports 
cross compilation to 2 Scala versions: 2.11 and 2.12. PySpark 2.4 supports Scala 2.11 by default so to build 
corresponding pantheon-core version need to select Scala 2.11 in sbt: 

```
shell> sbt

# selecting Scala 2.11 for the build:
sbt> ++ 2.11.12  

# To build fat Jar for Pantheon Core
# fat Jar will be created in core/target/scala-2.11 
sbt> core/assembly
```

## Acknowledgements

Many thanks to Contiamo and everyone there that supported this project.
