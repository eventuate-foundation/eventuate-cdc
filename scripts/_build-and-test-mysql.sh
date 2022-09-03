#! /bin/bash

export TERM=dumb

set -e

. ./scripts/set-env.sh

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

./gradlew ${GRADLE_OPTS} testClasses

docker="./gradlew ${DATABASE}Compose"

${docker}Down
${docker}Up


./gradlew $* -x :eventuate-local-java-cdc-connector-postgres-wal:test \
             -x :eventuate-local-java-cdc-connector-postgres-wal:test \
             -x :eventuate-local-java-cdc-connector-e2e-tests:test \
             -x :eventuate-tram-cdc-connector-e2e-tests:test \
             -x :eventuate-tram-cdc-connector-kafka-e2e-tests:test \
             -x :eventuate-tram-cdc-connector-activemq-e2e-tests:test \
             -x :eventuate-tram-cdc-connector-rabbitmq-e2e-tests:test \
             -x :eventuate-tram-cdc-connector-redis-e2e-tests:test \
             -x :eventuate-local-java-migration:test

${docker}Down
