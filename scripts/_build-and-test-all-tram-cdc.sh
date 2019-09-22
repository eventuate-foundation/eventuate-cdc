#!/bin/bash

set -e

if [ -z "$DOCKER_COMPOSE" ]; then
    echo setting DOCKER_COMPOSE
    export DOCKER_COMPOSE="docker-compose -f docker-compose-${DATABASE}.yml -f docker-compose-cdc-${DATABASE}-${MODE}.yml"
else
    echo using existing DOCKER_COMPOSE = $DOCKER_COMPOSE
fi

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

. ./scripts/set-env-${DATABASE}-${MODE}.sh

$DOCKER_COMPOSE stop
$DOCKER_COMPOSE rm --force -v

$DOCKER_COMPOSE build
$DOCKER_COMPOSE up -d ${DATABASE} zookeeper kafka

./scripts/wait-for-${DATABASE}.sh

$DOCKER_COMPOSE up -d cdcservice
./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test -Dtest.single=EventuateTramCdcKafkaTest

if [[ "${DATABASE}" != "mssql" ]]; then

    $DOCKER_COMPOSE stop kafka

    if [[ "${DATABASE}" == "mysql" ]]; then
        $DOCKER_COMPOSE up -d activemq
        $DOCKER_COMPOSE stop cdcservice
        $DOCKER_COMPOSE rm --force cdcservice

        #Testing cdc start with stopped database
        $DOCKER_COMPOSE stop mysql
        $DOCKER_COMPOSE rm --force mysql

        if [ -z "$SPRING_PROFILES_ACTIVE" ] ; then
          export SPRING_PROFILES_ACTIVE=ActiveMQ
        else
          export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE},ActiveMQ
        fi

        $DOCKER_COMPOSE up  -d cdcservice
        ./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

        # See whether waiting fixes CircleCI test failure

        sleep 30

        ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test -Dtest.single=EventuateTramCdcActiveMQTest

        $DOCKER_COMPOSE stop activemq
        $DOCKER_COMPOSE up -d rabbitmq
        $DOCKER_COMPOSE stop cdcservice
        $DOCKER_COMPOSE rm --force cdcservice

        export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/ActiveMQ/RabbitMQ}

        $DOCKER_COMPOSE up -d cdcservice
        ./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

        ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test -Dtest.single=EventuateTramCdcRabbitMQTest


        $DOCKER_COMPOSE stop rabbitmq
        export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/RabbitMQ/Redis}
    else
        if [ -z "$SPRING_PROFILES_ACTIVE" ] ; then
          export SPRING_PROFILES_ACTIVE=Redis
        else
          export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE},Redis
        fi
    fi

    $DOCKER_COMPOSE stop zookeeper
    $DOCKER_COMPOSE up -d redis
    $DOCKER_COMPOSE stop cdcservice
    $DOCKER_COMPOSE rm --force cdcservice


    $DOCKER_COMPOSE up -d cdcservice
    ./scripts/wait-for-services.sh $DOCKER_HOST_IP "actuator/health" 8099

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test -Dtest.single=EventuateTramCdcRedisTest

    $DOCKER_COMPOSE stop redis
    sleep 10
    $DOCKER_COMPOSE start redis

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-e2e-tests:cleanTest :eventuate-tram-cdc-connector-e2e-tests:test -Dtest.single=EventuateTramCdcRedisTest

fi

$DOCKER_COMPOSE stop
$DOCKER_COMPOSE rm --force -v

