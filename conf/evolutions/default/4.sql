# Create JDBC Product table

# --- !Ups
create table if not exists jdbc_products(
    jdbc_product_id serial primary key,
    name varchar not null unique,
    is_bundled boolean not null default 'false',
    product_root varchar not null,
    class_name varchar
);
alter table jdbc_products add unique (product_root);
alter sequence jdbc_products_jdbc_product_id_seq restart with 100;
insert into jdbc_products values (1, 'PostgreSQL', 'true', 'postgresql', null);
insert into jdbc_products values (2, 'MariaDB/MySQL', 'true', 'mysql', 'mariadb-java-client-2.2.3.jar');
insert into jdbc_products values (3, 'ClickHouse', 'true', 'clickhouse', 'ru.yandex.clickhouse.ClickHouseDriver');
insert into jdbc_products values (4, 'Google BigQuery', 'true', 'bigquery', 'com.simba.googlebigquery.jdbc42.Driver');
alter table data_sources add column jdbc_product_id integer references jdbc_products;
# --- !Downs
