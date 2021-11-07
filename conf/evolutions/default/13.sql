# --- !Ups
ALTER TABLE schemas DROP COLUMN display_name;
ALTER TABLE data_sources DROP COLUMN display_name;
ALTER TABLE catalogs RENAME COLUMN  display_name to name;
ALTER TABLE saved_queries RENAME COLUMN display_name to name;
ALTER TABLE backend_configs RENAME COLUMN display_name to name;
ALTER TABLE jdbc_products RENAME COLUMN display_name to name;
# --- !Downs