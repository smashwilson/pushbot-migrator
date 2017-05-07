#!/bin/bash

set -euo pipefail

source ./secrets.sh

prepare() {
  cat <<"EOM"
redis-cli save

mkdir -p ${HOME}/bundle/mappings/

sudo cp /var/lib/redis/dump.rdb ${HOME}/bundle/dump.rdb
sudo chown ash:ash ${HOME}/bundle/dump.rdb

cp /var/pushbot/botdata/quotes ${HOME}/bundle/quotes
cp /var/pushbot/botdata/lim.txt ${HOME}/bundle/lim.txt
cp /var/pushbot/botdata/mappings/*.json ${HOME}/bundle/mappings/

tar zcvf bundle.tar.gz bundle/
EOM
}

wait_for_postgres()
{
  TRIES=120
  while [ ${TRIES} -gt 0 ]; do
    printf '[%03d] Attempting PostgreSQL connection: ' ${TRIES}
    if ./psql.sh --quiet --command='SELECT 1;' >/dev/null 2>&1 ; then
      printf "ok\n"
      return 0
    fi

    printf 'no\n'
    sleep 0.5
    (( TRIES-- ))
  done
  printf 'Unable to connect to PostgreSQL.\n' >&2
  return 1
}

if [ "${1:-}" = "reset" ]; then
  eval $(ssh-agent)
  trap "kill ${SSH_AGENT_PID}" EXIT
  ssh-add

  prepare | ssh ash@azurefire.net /bin/bash
  scp ash@azurefire.net:bundle.tar.gz ./bundle.tar.gz
  tar zxvf bundle.tar.gz

  if docker container inspect redis >/dev/null 2>&1 ; then
    printf "Terminating existing Redis container:\n"
    docker stop redis
    docker rm redis
    printf " ... success.\n"
  fi

  if docker container inspect postgres >/dev/null 2>&1 ; then
    printf "Terminating existing PostgreSQL container:\n"
    docker stop postgres
    docker rm postgres
    printf " ... success.\n"
  fi

  printf "Starting a new Redis container:\n"
  docker run -d \
    --name redis \
    --publish 6379:6379 \
    --volume $(cygpath --windows $(pwd))/bundle:/data \
    redis:3.2.8
  printf " ... success.\n"

  printf "Starting a new PostgreSQL container:\n"
  docker run -d \
    --name postgres \
    --publish 5432:5432 \
    --env POSTGRES_DB=pushbot \
    --env POSTGRES_USER=${PG_USER} \
    --env POSTGRES_PASSWORD=${PG_PASS} \
    postgres:9.6
  printf " ... success.\n"

  wait_for_postgres
fi

time node cli.js \
  --pg postgres://${PG_USER}:${PG_PASS}@${PG_ADDR}:5432/pushbot \
  --brain --markov --quote --mapping \
  --transfer
