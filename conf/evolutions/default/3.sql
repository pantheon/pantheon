# Schema psl should not be null

# --- !Ups
UPDATE schemas
    SET psl =
        CASE WHEN psl IS NULL THEN 'schema ' || name
        ELSE psl
        END;

ALTER TABLE schemas ALTER COLUMN psl SET NOT NULL;
# --- !Downs
