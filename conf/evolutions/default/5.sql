# --- !Ups
CREATE TABLE IF NOT EXISTS query_history(
  query_history_id UUID PRIMARY KEY,
  query_type VARCHAR NOT NULL,
  started_at TIMESTAMP NOT NULL,
  query_description VARCHAR NOT NULL,
  catalog_id  INTEGER NOT NULL,
  completion_status VARCHAR,
  completed_at TIMESTAMP,
  plan VARCHAR,
  backend_logical_plan VARCHAR,
  backend_physical_plan VARCHAR
);
# --- !Downs