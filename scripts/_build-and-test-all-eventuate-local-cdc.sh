#!/bin/bash

set -e

if [ -z "$DOCKER_COMPOSE" ]; then
    echo setting DOCKER_COMPOSE
    export DOCKER_COMPOSE="docker-compose -f docker-compose-${database}.yml -f docker-compose-cdc-${database}-${mode}.yml"
else
    echo using existing DOCKER_COMPOSE = $DOCKER_COMPOSE
fi

export EVENTUATE_CDC_TYPE=EventuateLocal

./gradlew $GRADLE_OPTIONS $* :eventuate-tram-cdc-mysql-service:clean :eventuate-tram-cdc-mysql-service:assemble


if [[ "${database}" == "mariadb" ]]; then
    . ./scripts/set-env-mysql-binlog.sh
else
    . ./scripts/set-env-${database}-${mode}.sh
fi

$DOCKER_COMPOSE down -v --remove-orphans

$DOCKER_COMPOSE build
$DOCKER_COMPOSE up -d ${database}

echo waiting for database

if [[ "${database}" == "mariadb" ]]; then
    ./scripts/wait-for-mysql.sh
else
    ./scripts/wait-for-${database}.sh
fi

$DOCKER_COMPOSE up -d

./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

# Assert healthcheck good

echo testing database and zookeeper restart scenario $(date)

$DOCKER_COMPOSE stop kafka
$DOCKER_COMPOSE stop zookeeper ${database}
sleep 10

$DOCKER_COMPOSE start zookeeper ${database}
if [[ "${database}" == "mariadb" ]]; then
    ./scripts/wait-for-mysql.sh
else
    ./scripts/wait-for-${database}.sh
fi

$DOCKER_COMPOSE start kafka
sleep 10

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

$DOCKER_COMPOSE down -v --remove-orphans
