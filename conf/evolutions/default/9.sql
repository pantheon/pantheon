# --- !Ups
ALTER TABLE query_history ADD COLUMN custom_reference VARCHAR;

-- making enums serialization consistent
UPDATE saved_queries SET options=regexp_replace(options, ':"desc"', ':"Desc"');
UPDATE saved_queries SET options=regexp_replace(options, ':"asc"', ':"Asc"');
# --- !Downs