#!/bin/bash

set -e

. ./scripts/set-env.sh

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

export MODE=unified

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown



echo TESTING KAFKA MYSQL BINLOG



./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:cleanTest :eventuate-tram-cdc-connector-kafka-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown



echo TESTING KAFKA POSTGRES POLLING
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:cleanTest :eventuate-tram-cdc-connector-kafka-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown



echo TESTING KAFKA POSTGRES WAL
export SPRING_PROFILES_ACTIVE=postgres,PostgresWal
export SPRING_DATASOURCE_URL=jdbc:postgresql://${DOCKER_HOST_IP}:5433/eventuate

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:cleanTest :eventuate-tram-cdc-connector-kafka-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown