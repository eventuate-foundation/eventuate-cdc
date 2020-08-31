#! /bin/bash

export TERM=dumb

set -e

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

docker="./gradlew postgresCompose"

${docker}Down

./gradlew ${GRADLE_OPTS} $* testClasses

. ./scripts/set-env.sh
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling

${docker}Up

./gradlew $* :eventuate-local-java-cdc-connector-polling:cleanTest :eventuate-local-java-cdc-connector-polling:test --tests=io.eventuate.local.polling.PollingDaoIntegrationTest

${docker}Down

