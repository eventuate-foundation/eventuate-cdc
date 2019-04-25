#!/bin/bash

set -e

. ./scripts/_set-env.sh

if [ -z "$DOCKER_COMPOSE" ]; then
    echo setting DOCKER_COMPOSE
    export DOCKER_COMPOSE="docker-compose -f docker-compose-unified.yml -f docker-compose-cdc-unified.yml"
else
    echo using existing DOCKER_COMPOSE = $DOCKER_COMPOSE
fi


./gradlew $GRADLE_OPTIONS $* :eventuate-tram-cdc-mysql-service:clean :eventuate-tram-cdc-mysql-service:assemble

$DOCKER_COMPOSE stop
$DOCKER_COMPOSE rm --force -v

$DOCKER_COMPOSE build
$DOCKER_COMPOSE up -d mysqlbinlogpipeline postgrespollingpipeline postgreswalpipeline

./scripts/wait-for-mysql.sh
./scripts/wait-for-postgres.sh
export POSTGRES_PORT=5433
./scripts/wait-for-postgres.sh

$DOCKER_COMPOSE up -d
./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

echo TESTING KAFKA MYSQL BINLOG

. ./scripts/set-env-mysql-binlog.sh

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test  -Dtest.single=EventuateTramCdcKafkaTest

echo TESTING KAFKA POSTGRES POLLING
. ./scripts/set-env-postgres-polling.sh

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test  -Dtest.single=EventuateTramCdcKafkaTest
echo TESTING KAFKA POSTGRES WAL

. ./scripts/set-env-postgres-wal.sh

export SPRING_DATASOURCE_URL=jdbc:postgresql://${DOCKER_HOST_IP}:5433/eventuate

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test
./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test  -Dtest.single=EventuateTramCdcKafkaTest

$DOCKER_COMPOSE stop
$DOCKER_COMPOSE rm --force -v

