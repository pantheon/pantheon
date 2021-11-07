# Create Saved Queries

# --- !Ups

CREATE TABLE IF NOT EXISTS saved_queries(
  saved_query_id serial PRIMARY KEY,
  catalog_id integer REFERENCES catalogs NOT NULL,
  schema_name VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  query_type VARCHAR NOT NULL,
  options VARCHAR NOT NULL,

  UNIQUE (catalog_id, name)
);

# --- !Downs
