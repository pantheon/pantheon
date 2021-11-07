# --- !Ups
update saved_queries set base = jsonb_set(
   base::jsonb,
   '{type}',
   CASE
   WHEN base::jsonb @> '{"type":"Olap"}'::jsonb THEN '"Aggregate"'::jsonb
   WHEN base::jsonb @> '{"type":"Entity"}'::jsonb THEN '"Record"'::jsonb
   ELSE base::jsonb->'type'
   END
   );

update query_history set query = CASE
   WHEN type = 'Schema' THEN (
       jsonb_set(
           query::jsonb,
           '{type}',
           CASE
           WHEN query::jsonb @> '{"type":"Olap"}'::jsonb THEN '"Aggregate"'::jsonb
           WHEN query::jsonb @> '{"type":"Entity"}'::jsonb THEN '"Record"'::jsonb
           ELSE query::jsonb->'type'
           END
           )
   		)::varchar
  ELSE query
  END;

# --- !Downs