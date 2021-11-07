# --- !Ups
--This is the breaking migration. Truncating query_history and saved_queries before applying.
TRUNCATE TABLE query_history;
TRUNCATE TABLE saved_queries;
ALTER TABLE query_history RENAME COLUMN query_description to query;
ALTER TABLE query_history DROP COLUMN query_type;
ALTER TABLE query_history ADD COLUMN data_source_id uuid;
ALTER TABLE query_history ADD COLUMN schema_id uuid;
ALTER TABLE query_history ADD COLUMN type VARCHAR NOT NULL;

ALTER TABLE saved_queries RENAME COLUMN schema_name to schema_id;
ALTER TABLE saved_queries ALTER COLUMN schema_id TYPE uuid USING schema_id::uuid;
ALTER TABLE saved_queries ADD CONSTRAINT FK_Schema_Id FOREIGN KEY (schema_id) REFERENCES schemas ON DELETE CASCADE;

# --- !Downs
