#! /bin/bash

export TERM=dumb

set -e

if [ -z "$ACTUAL_DATABASE" ]; then
    export ACTUAL_DATABASE=${DATABASE}
fi

. ./scripts/set-env.sh

docker="./gradlew ${ACTUAL_DATABASE}jsonCompose"

${docker}Down

${docker}Up

./gradlew :${TEST_MODULE}:cleanTest ${TEST_MODULE}:test --tests "${TEST_CLASS}"

${docker}Down
