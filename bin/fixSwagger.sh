#!/bin/bash

docker run -v $(pwd)/docs/swagger.json:/swagger.json contiamo/pantheon-swagger-fixer:latest

exit $?
