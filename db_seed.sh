#!/bin/bash

[ -z "$1" ] && echo "Pantheon server required as a parameter, exiting" && exit 1

BASE_URL=$1

function createReq() {
  local url=$1 
  local data=$2

  curl "${BASE_URL}${url}" -X POST -H 'content-type: application/json' --data "${data}"
}

# create catalog
CAT_ID="247b21bd-3bad-4ff2-b216-fbe9e51a8c49"

echo "Creating catalog"
createReq "/catalogs" "{\"name\":\"Test company\",\"id\":\"${CAT_ID}\"}"
echo

# create datasource
DS_ID="1b67aee9-b82d-4b89-9c02-06f3d320b1f8"
DS_LOC="jdbc:postgresql://localhost/foodmart"
DS_PARAMS='{"host":"localhost","database":"foodmart","user":"foodmart","password":"foodmart"}'

echo "Creating datasource"
createReq "/catalogs/${CAT_ID}/dataSources" "{\"dataSourceProductId\":\"45b8a9da-3bff-4e2f-83b3-8cf9a7e6daf1\",\"name\":\"foodmart\",\"properties\":${DS_PARAMS}}"
echo

# create schema
S_ID="590c0fb2-b59e-48d9-86bc-3f489207cdec"
PSL=$(sed 's/SCHEMA_NAME/sales/' seed.psl | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g' | tr '\t' ' ' )

echo "Creating schema"
createReq "/catalogs/${CAT_ID}/schemas" "{\"name\":\"sales\",\"psl\":\"${PSL}\"}"
echo


