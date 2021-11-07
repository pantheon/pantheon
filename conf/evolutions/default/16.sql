# --- !Ups

alter table jdbc_products rename to data_source_products;
alter table jdbc_product_jars rename to data_source_product_jars;
alter table data_source_products rename jdbc_product_id to data_source_product_id;
alter table data_source_products alter column name set not null;
alter table data_source_products add column type varchar not null default 'jdbc';
alter table data_source_products alter column type drop default;

create table data_source_product_icons(
  data_source_product_id uuid primary key references data_source_products on delete cascade,
  contents bytea not null
);

create table data_source_product_properties(
  data_source_product_property_id uuid primary key,
  data_source_product_id uuid not null references data_source_products,
  name varchar not null,
  type varchar not null,
  order_index integer not null
);

update roles set resource_type = 'DataSourceProduct' where resource_type = 'JdbcProduct';

alter table data_sources rename jdbc_product_id to data_source_product_id;
alter table data_sources rename params to properties;
alter table data_source_product_jars rename jdbc_product_id to data_source_product_id;

update data_sources set properties = jsonb_set(properties::jsonb, '{url}',('"' || location_uri || '"')::jsonb, true);

update data_sources set data_source_product_id = (
    select data_source_product_id from data_source_products where product_root = 'postgresql')
where location_uri like '%postgresql%';

update data_sources set data_source_product_id = (
    select data_source_product_id from data_source_products where product_root = 'clickhouse')
where location_uri like '%clickhouse%';

update data_sources set data_source_product_id = (
    select data_source_product_id from data_source_products where product_root = 'bigquery')
where location_uri like '%bigquery%';

alter table data_sources alter column data_source_product_id set not null;
alter table data_sources drop column type;
alter table data_sources drop column location_uri;

# --- !Downs
