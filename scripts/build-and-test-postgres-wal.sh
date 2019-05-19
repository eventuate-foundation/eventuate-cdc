#! /bin/bash

export TERM=dumb

set -e

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

./gradlew ${GRADLE_OPTS} $* testClasses

. ./scripts/set-env-postgres-wal.sh

docker-compose -f docker-compose-postgres.yml build
docker-compose -f docker-compose-postgres.yml  up -d

./scripts/wait-for-postgres.sh

./gradlew $* :eventuate-local-java-cdc-connector-postgres-wal:cleanTest :eventuate-local-java-cdc-connector-postgres-wal:test

docker-compose -f docker-compose-postgres.yml down -v --remove-orphans
