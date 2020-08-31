#!/bin/bash

set -e

docker="./gradlew ${database}${mode}Compose"

export EVENTUATE_CDC_TYPE=EventuateLocal

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

. ./scripts/set-env.sh

${docker}Down
${docker}Up

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

echo testing database and zookeeper restart scenario $(date)

export COMPOSE_SERVICES="zookeeper,kafka,${database}"

${docker}Down
sleep 10
${docker}Up

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

export COMPOSE_SERVICES=""

${docker}Down
