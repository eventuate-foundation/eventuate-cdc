#! /bin/bash -e

docker login -u ${DOCKER_USER_ID?} -p ${DOCKER_PASSWORD?}

./gradlew :eventuate-cdc-service:assemble

./eventuate-cdc-service/build-docker-multi-arch.sh
