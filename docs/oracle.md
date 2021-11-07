# How to test Oracle Dialect Locally

## Pull Docker Image

From Contiamo repo:

```bash
docker pull eu.gcr.io/dev-and-test-env/oracle-database:18.4.0-xe
docker tag eu.gcr.io/dev-and-test-env/oracle-database:18.4.0-xe oracle/database:18.4.0-xe
```

Or build yourself from https://github.com/oracle/docker-images

```bash
git clone https://github.com/oracle/docker-images
cd docker-images
cd OracleDatabase/SingleInstance/dockerfiles
./buildDockerImage.sh -v 18.4.0 -x
```

## Run Oracle container (at least 2Gb free RAM required)

You need to create dir to store Oracle data between container runs:

```bash
sudo mkdir /srv/work/oracle
sudo chmod 0777 /srv/work/oracle
```

Run docker container:

```bash
docker run -p 1521:1521 -e ORACLE_PWD=pass -v /srv/work/oracle:/opt/oracle/oradata oracle/database:18.4.0-xe
```

First run is going to take 10-20 mins, so brace yourself.

## Import Foodmart dataset

Use https://github.com/contiamo/foodmart-data loader.

```bash
git clone https://github.com/contiamo/foodmart-data
cd foodmart-data
./FoodMartLoader.sh --db oracle --db-pass pass
```

## Configure and run Pantheon

In `core/src/test/resources/test.conf` enable `oracle` data source and disable
`hsqldb` (optionally):

```
enabled = true
```

Run `sbt`:

```bash
ORACLE_HOST=localhost ORACLE_PORT=1521 ORACLE_USER=SYSTEM ORACLE_PASSWORD=pass sbt 'testOnly pantheon.PantheonSpec -- -eU'
```
