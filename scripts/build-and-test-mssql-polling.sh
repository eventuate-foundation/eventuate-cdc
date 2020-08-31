#! /bin/bash

export TERM=dumb

set -e

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

. ./scripts/set-env.sh

docker="./gradlew mssqlCompose"

${docker}Down

export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling

./gradlew ${GRADLE_OPTS} $* testClasses

${docker}Up

./gradlew $* :eventuate-local-java-cdc-connector-polling:cleanTest
./gradlew $* :eventuate-local-java-cdc-connector-polling:test --tests=io.eventuate.local.polling.PollingCdcProcessorEventsTest
./gradlew $* :eventuate-local-java-cdc-connector-polling:test --tests=io.eventuate.local.polling.PollingDaoIntegrationTest

${docker}Down
