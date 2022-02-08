#!/bin/bash

set -e

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

. ./scripts/set-env.sh

export DOCKER_ENV_FILE=docker-compose-env-files/kafka.env

./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown

./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:cleanTest :eventuate-tram-cdc-connector-kafka-e2e-tests:test

if [[ "${DATABASE}" == "mssql" ]]; then
  export TEST_MESSAGE_CLEANER=true
  ./gradlew cleanTest :eventuate-cdc-common-e2e-tests:test
fi

./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-kafka-e2e-tests:tramcdcComposeDown

unset DOCKER_ENV_FILE

if [[ "${DATABASE}" == "postgres"  || "${DATABASE}" == "mysql" ]] ; then
  if [[ "${MODE}" == "polling" ||  "${MODE}" == "binlog" || "${MODE}" == "binlog-multi-arch" ]] ; then

    # if this is Arm platform exit 0

    if [ -z "$SPRING_PROFILES_ACTIVE" ] ; then
      export SPRING_PROFILES_ACTIVE=ActiveMQ
    else
      export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE},ActiveMQ
    fi

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-activemq-e2e-tests:cleanTest :eventuate-tram-cdc-connector-activemq-e2e-tests:test
    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-activemq-e2e-tests:tramcdcComposeDown

    export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/ActiveMQ/RabbitMQ}

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-rabbitmq-e2e-tests:cleanTest :eventuate-tram-cdc-connector-rabbitmq-e2e-tests:test
    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-rabbitmq-e2e-tests:tramcdcComposeDown

    export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE/RabbitMQ/Redis}

    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-redis-e2e-tests:cleanTest :eventuate-tram-cdc-connector-redis-e2e-tests:test
    ./gradlew $GRADLE_OPTIONS :eventuate-tram-cdc-connector-redis-e2e-tests:tramcdcComposeDown
  fi
fi


