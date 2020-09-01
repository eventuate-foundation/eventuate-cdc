#!/bin/bash

export E2EMigrationTest=true

set -e

docker="./gradlew mysqlmigrationCompose"

./gradlew assemble

${docker}Down

${docker}Up -P composeServices=old-cdc-service

./gradlew :eventuate-local-java-migration:cleanTest
./gradlew eventuate-local-java-migration:test --tests "io.eventuate.local.cdc.debezium.migration.MigrationOldCdcPhaseE2ETest"

${docker}Down -P composeServices=old-cdc-service
${docker}Up -P composeServices=eventuate-cdc-service

./gradlew eventuate-local-java-migration:test --tests "io.eventuate.local.cdc.debezium.migration.MigrationNewCdcPhaseE2ETest"

${docker}Down