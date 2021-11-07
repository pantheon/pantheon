# --- !Ups

create table if not exists jdbc_product_jars(
    jdbc_product_id uuid not null references jdbc_products(jdbc_product_id),
    name varchar not null,
    content bytea not null,
    primary key (jdbc_product_id, name)
);

# --- !Downs
