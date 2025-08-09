#! /bin/bash -e

docker login -u ${DOCKER_USER_ID?} -p ${DOCKER_PASSWORD?}

./gradlew publishMultiArchContainerImages \
  -P containerNames=eventuateio/eventuate-cdc-service  \
  -P "multiArchTag=${MULTI_ARCH_TAG?}" --info