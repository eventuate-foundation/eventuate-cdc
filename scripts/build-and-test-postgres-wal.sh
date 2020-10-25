#! /bin/bash

export TERM=dumb

set -e

GRADLE_OPTS=""

if [ "$1" = "--clean" ] ; then
  GRADLE_OPTS="clean"
  shift
fi

docker="./gradlew postgresCompose"
${docker}Down

./gradlew ${GRADLE_OPTS} $* testClasses

. ./scripts/set-env.sh

${docker}Up

./gradlew $* :eventuate-local-java-cdc-connector-postgres-wal:cleanTest :eventuate-local-java-cdc-connector-postgres-wal:test

${docker}Down