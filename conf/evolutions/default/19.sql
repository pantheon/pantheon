# --- !Ups

insert into data_source_product_properties
select uuid_generate_v4(), data_source_product_id, 'url', 'text', 1
from  data_source_products dsp
where dsp.product_root in ('postgresql', 'mysql', 'clickhouse', 'bigquery');

insert into data_source_product_properties
select uuid_generate_v4(), data_source_product_id, 'username', 'text', 2
from  data_source_products dsp
where dsp.product_root in ('postgresql', 'mysql', 'clickhouse');

insert into data_source_product_properties
select uuid_generate_v4(), data_source_product_id, 'password', 'password', 3
from  data_source_products dsp
where dsp.product_root in ('postgresql', 'mysql', 'clickhouse');

# --- !Downs
