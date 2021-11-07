# --- !Ups
ALTER TABLE saved_queries DROP COLUMN query_type;
ALTER TABLE saved_queries RENAME COLUMN options to base;
# --- !Downs
