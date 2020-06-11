#! /bin/bash

export TERM=dumb

set -e

if [ -z "$ACTUAL_DATABASE" ]; then
    export ACTUAL_DATABASE=${DATABASE}
fi

. ./scripts/set-env-${DATABASE}-${MODE}.sh

DOCKER_COMPOSE="docker-compose -f docker-compose-${ACTUAL_DATABASE}-json.yml"

$DOCKER_COMPOSE down

$DOCKER_COMPOSE up --build -d

./scripts/wait-for-${DATABASE}.sh

./gradlew :${TEST_MODULE}:cleanTest ${TEST_MODULE}:test --tests "${TEST_CLASS}"

$DOCKER_COMPOSE down
