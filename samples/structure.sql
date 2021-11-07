BEGIN;

CREATE TABLE IF NOT EXISTS catalog(
  id UUID,
  "key" VARCHAR,

  name VARCHAR
);

CREATE TABLE IF NOT EXISTS catalog_data_sources(
  id UUID,
  catalog_id UUID,
  data_source_id UUID,
  "key" VARCHAR
);

CREATE TABLE IF NOT EXISTS data_sources(
  id UUID,

  name VARCHAR,
  backend VARCHAR,
  location_uri VARCHAR,
  config VARCHAR
);

CREATE TABLE IF NOT EXISTS tables(
  id UUID,
  data_source_id UUID,
  "key" VARCHAR(255),

  storage_key VARCHAR(255),
  config VARCHAR,
  meta_name VARCHAR,
  meta_description VARCHAR
);

CREATE TABLE IF NOT EXISTS columns(
  id UUID,
  table_id UUID,
  "key" VARCHAR(255),

  storage_key VARCHAR(255),
  meta_name VARCHAR,
  meta_description VARCHAR
);

CREATE TABLE IF NOT EXISTS engines(
  id UUID,
  "key" VARCHAR(255),

  name VARCHAR,
  backend VARCHAR,
  location_uri VARCHAR
);

COMMIT;