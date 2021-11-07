#!/bin/bash

DB=pantheon_development
FILE=conf/structure.sql

pg_dump -s -x -O $DB > $FILE

# dump evolutions table, removing SET commands and comments
pg_dump --table play_evolutions --column-inserts --data-only -x -O --schema=public $DB >> $FILE

