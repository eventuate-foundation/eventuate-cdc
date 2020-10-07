#!/bin/bash

set -e

. ./scripts/set-env.sh

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

export MODE=unified

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown

function runE2ETests() {
  ./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
  ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:cleanTest :eventuate-tram-cdc-connector-kafka-e2e-tests:test
}

echo "TESTING KAFKA MYSQL BINLOG"
runE2ETests

echo "TESTING KAFKA POSTGRES POLLING"
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
runE2ETests

echo "TESTING KAFKA POSTGRES WAL"
export SPRING_PROFILES_ACTIVE=postgres,PostgresWal
export SPRING_DATASOURCE_URL=jdbc:postgresql://${DOCKER_HOST_IP}:5433/eventuate
runE2ETests

unset SPRING_PROFILES_ACTIVE
unset SPRING_DATASOURCE_URL

export EVENTUATE_OUTBOX_ID=1
echo "TESTING KAFKA MYSQL BINLOG (DATABASE ID)"
runE2ETests

export EVENTUATE_OUTBOX_ID=2
echo "TESTING KAFKA POSTGRES POLLING (DATABASE ID)"
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
runE2ETests

export EVENTUATE_OUTBOX_ID=3
echo "TESTING KAFKA POSTGRES WAL (DATABASE ID)"
export SPRING_PROFILES_ACTIVE=postgres,PostgresWal
export SPRING_DATASOURCE_URL=jdbc:postgresql://${DOCKER_HOST_IP}:5433/eventuate
runE2ETests