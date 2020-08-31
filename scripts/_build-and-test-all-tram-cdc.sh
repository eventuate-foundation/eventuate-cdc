#!/bin/bash

set -e

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

. ./scripts/set-env.sh

docker="./gradlew tram${DATABASE}${MODE}Compose"

export COMPOSE_SERVICES=","
${docker}Down

export COMPOSE_SERVICES="${DATABASE},zookeeper,kafka,eventuate-cdc-service"
${docker}Up

if [[ "${DATABASE}" == "mysql" ]]; then
    export COMPOSE_SERVICES="redis,activemq,rabbitmq"
else
    export COMPOSE_SERVICES="redis"
fi

if [[ "${DATABASE}" != "mssql" ]]; then
    ${docker}Down
fi

./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test --tests=io.eventuate.tram.connector.EventuateTramCdcKafkaTest


if [[ "${DATABASE}" != "mssql" ]]; then

    export COMPOSE_SERVICES="eventuate-cdc-service"
    ${docker}Down

    if [[ "${DATABASE}" == "mysql" ]]; then

        if [ -z "$SPRING_PROFILES_ACTIVE" ] ; then
          export SPRING_PROFILES_ACTIVE=ActiveMQ
        else
          export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE},ActiveMQ
        fi

        export COMPOSE_SERVICES="eventuate-cdc-service"
        ${docker}Up

        export COMPOSE_SERVICES="redis,kafka,rabbitmq"
        ${docker}Down

        ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test --tests=io.eventuate.tram.connector.EventuateTramCdcActiveMQTest

        export COMPOSE_SERVICES="eventuate-cdc-service"
        ${docker}Down

        export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/ActiveMQ/RabbitMQ}

        export COMPOSE_SERVICES="eventuate-cdc-service"
        ${docker}Up

        export COMPOSE_SERVICES="redis,kafka,activemq"
        ${docker}Down

        ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test --tests=io.eventuate.tram.connector.EventuateTramCdcRabbitMQTest

        export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/RabbitMQ/Redis}

        export COMPOSE_SERVICES="eventuate-cdc-service"
        ${docker}Down
    else
        if [ -z "$SPRING_PROFILES_ACTIVE" ] ; then
          export SPRING_PROFILES_ACTIVE=Redis
        else
          export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE},Redis
        fi
    fi

    export COMPOSE_SERVICES="eventuate-cdc-service"
    ${docker}Up

    if [[ "${DATABASE}" == "mysql" ]]; then
        export COMPOSE_SERVICES="rabbitmq,kafka,activemq"
    else
        export COMPOSE_SERVICES="kafka"
    fi
    ${docker}Down

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test --tests=io.eventuate.tram.connector.EventuateTramCdcRedisTest

    export COMPOSE_SERVICES="redis"

    ${docker}Down
    sleep 10
    ${docker}Up

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test --tests=io.eventuate.tram.connector.EventuateTramCdcRedisTest
fi

export COMPOSE_SERVICES=","
${docker}Down

