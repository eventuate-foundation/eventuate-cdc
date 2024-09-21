#!/bin/bash

set -e

export EVENTUATE_CDC_TYPE=EventuateLocal

./gradlew $GRADLE_OPTIONS $* :eventuate-cdc-service:clean :eventuate-cdc-service:assemble

. ./scripts/set-env.sh

./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

# echo testing database and zookeeper restart scenario $(date)

# ./gradlew -P composeServices=zookeeper,kafka,${DATABASE} :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
# sleep 10

# ./gradlew $GRADLE_OPTIONS :eventuate-local-java-cdc-connector-e2e-tests:cleanTest :eventuate-local-java-cdc-connector-e2e-tests:test

# ./gradlew :eventuate-local-java-cdc-connector-e2e-tests:eventuatelocalcdcComposeDown
