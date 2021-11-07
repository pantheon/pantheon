# Create initial tables

# --- !Ups
CREATE TABLE IF NOT EXISTS catalogs(
  catalog_id serial PRIMARY KEY,

  name VARCHAR NOT NULL,
  display_name VARCHAR,
  description VARCHAR
);

CREATE TABLE IF NOT EXISTS schemas(
  schema_id serial PRIMARY KEY,
  catalog_id integer REFERENCES catalogs NOT NULL,

  name VARCHAR NOT NULL,
  display_name VARCHAR,
  description VARCHAR,
  psl VARCHAR,
  valid boolean
);

CREATE TABLE IF NOT EXISTS schema_usages(
  parent_id integer REFERENCES schemas(schema_id) NOT NULL,
  child_id integer REFERENCES schemas(schema_id) NOT NULL
);

CREATE TABLE IF NOT EXISTS data_sources(
  data_source_id serial PRIMARY KEY,
  catalog_id integer REFERENCES catalogs NOT NULL,

  name VARCHAR NOT NULL,
  display_name VARCHAR,
  description VARCHAR,
  typ VARCHAR NOT NULL,
  location_uri VARCHAR,
  params VARCHAR NOT NULL,

  UNIQUE (catalog_id, name)
);

CREATE TABLE IF NOT EXISTS data_source_links(
  schema_id integer REFERENCES schemas NOT NULL,
  data_source_id integer REFERENCES data_sources NOT NULL,

  PRIMARY KEY (schema_id, data_source_id)
);

-- tbls not tables to avoid naming conflict when loading with calcite
CREATE TABLE IF NOT EXISTS tbls(
  tbl_id serial PRIMARY KEY,
  data_source_id integer REFERENCES data_sources NOT NULL,

  name VARCHAR NOT NULL,
  display_name VARCHAR,
  description VARCHAR,
  params VARCHAR NOT NULL
);

-- cols not columns to avoid naming conflict when loading with calcite
CREATE TABLE IF NOT EXISTS cols(
  tbl_col_id serial PRIMARY KEY,
  tbl_id integer REFERENCES tbls NOT NULL,

  name VARCHAR NOT NULL,
  display_name VARCHAR,
  description VARCHAR,
  typ VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS constraint_col_lists(
  col_list_id serial PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS key_constraints(
  key_constraint_id serial PRIMARY KEY,
  child_col_list_id integer REFERENCES constraint_col_lists(col_list_id) NOT NULL,
  child_tbl_id integer REFERENCES tbls(tbl_id) NOT NULL,
  parent_col_list_id integer REFERENCES constraint_col_lists(col_list_id) NOT NULL,
  parent_tbl_id integer REFERENCES tbls(tbl_id) NOT NULL,

  name VARCHAR NOT NULL,
  typ VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS constraint_cols(
  col_id serial PRIMARY KEY,
  col_list_id integer REFERENCES constraint_col_lists NOT NULL,

  name VARCHAR NOT NULL,
  idx integer NOT NULL
);

# --- !Downs

