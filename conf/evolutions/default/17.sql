# --- !Ups

UPDATE schemas SET psl = regexp_replace(psl, '(schema [^(]*)\(', E'\\1(strict=false, ')

# --- !Downs
