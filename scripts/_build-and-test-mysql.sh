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

docker="./gradlew ${database}Compose"

${docker}Down
${docker}Up


./gradlew $* -x :eventuate-local-java-cdc-connector-postgres-wal:test -x :eventuate-local-java-cdc-connector-postgres-wal:test -x eventuate-local-java-cdc-connector-e2e-tests:test -x eventuate-tram-cdc-connector-e2e-tests:test

${docker}Down
