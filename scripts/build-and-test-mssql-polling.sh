#! /bin/bash

export TERM=dumb

set -e

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

./gradlew ${GRADLE_OPTS} $* testClasses

. ./scripts/set-env-mssql-polling.sh

docker-compose -f docker-compose-mssql.yml  up --build -d

./scripts/wait-for-mssql.sh

./gradlew $* :eventuate-local-java-cdc-connector-polling:cleanTest
./gradlew $* :eventuate-local-java-cdc-connector-polling:test -Dtest.single=PollingCdcProcessorEventsTest
./gradlew $* :eventuate-local-java-cdc-connector-polling:test -Dtest.single=PollingDaoIntegrationTest

docker-compose -f docker-compose-mssql.yml down -v --remove-orphans
