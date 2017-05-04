#!/bin/bash

source ./secrets.sh

exec docker run -it \
  --rm --link postgres:postgres \
  --env PGPASSWORD=${PG_PASS} \
  --env PGDATABASE=pushbot \
  postgres:9.6 psql -h postgres -U ${PG_USER} "$@"
