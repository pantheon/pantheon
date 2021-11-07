# Create BackendConfig entity

# --- !Ups
create table if not exists backend_configs(
    backend_config_id serial primary key,
    catalog_id integer REFERENCES catalogs NOT NULL,
    display_name varchar not null,
    backend_type varchar not null,
    description varchar,
    params varchar not null
);

alter table catalogs add column backend_config_id integer references backend_configs;
alter table schemas add column backend_config_id integer references backend_configs;
# --- !Downs
