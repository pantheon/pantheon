# --- !Ups

-- 1)Only Schema and DataSource require a name property which is used for references (schemas importing other schemas, schemas using a datasource)
-- 2)No other resources should use the name property to avoid confusion.
-- 3)All resources displayed in the UI should have the properties displayName and description.

UPDATE catalogs SET display_name = name;
ALTER TABLE catalogs DROP COLUMN name;

ALTER TABLE jdbc_products ADD COLUMN display_name VARCHAR;
ALTER TABLE jdbc_products ADD COLUMN description VARCHAR;
UPDATE jdbc_products SET display_name = name;
ALTER TABLE jdbc_products DROP COLUMN name;

ALTER TABLE backend_configs ALTER COLUMN display_name DROP NOT NULL;

ALTER TABLE saved_queries ADD COLUMN display_name VARCHAR;
ALTER TABLE saved_queries ADD COLUMN description VARCHAR;
UPDATE saved_queries SET display_name = name;
ALTER TABLE saved_queries DROP COLUMN name;

# --- !Downs
