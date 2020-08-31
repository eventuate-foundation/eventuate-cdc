#!/bin/bash

export E2EMigrationTest=true

set -e

docker="./gradlew mysqlmigrationCompose"

./gradlew assemble
export COMPOSE_SERVICES=","
${docker}Down

export COMPOSE_SERVICES="old-cdc-service"
${docker}Up

./gradlew :eventuate-local-java-migration:cleanTest
./gradlew eventuate-local-java-migration:test --tests "io.eventuate.local.cdc.debezium.migration.MigrationOldCdcPhaseE2ETest"

${docker}Down
export COMPOSE_SERVICES="eventuate-cdc-service"
${docker}Up

./gradlew eventuate-local-java-migration:test --tests "io.eventuate.local.cdc.debezium.migration.MigrationNewCdcPhaseE2ETest"

export COMPOSE_SERVICES=","
${docker}Down