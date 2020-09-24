#! /bin/bash

export TERM=dumb

set -e

if [ -z "$ACTUAL_DATABASE" ]; then
    export ACTUAL_DATABASE=${DATABASE}
fi

. ./scripts/set-env.sh

docker="./gradlew ${ACTUAL_DATABASE}databaseidCompose"

${docker}Down
${docker}Up

echo "TESTING APPLICATION GENERATION ID WITH DATABASE WITH XID"
./gradlew :${TEST_MODULE}:cleanTest ${TEST_MODULE}:test --tests "${TEST_CLASS}"

${docker}Down

${docker}Down
${docker}Up

export EVENTUATELOCAL_CDC_READER_ID=1

echo "TESTING DATABASE GENERATION ID"
./gradlew :${TEST_MODULE}:cleanTest ${TEST_MODULE}:test --tests "${TEST_CLASS}"

${docker}Down
