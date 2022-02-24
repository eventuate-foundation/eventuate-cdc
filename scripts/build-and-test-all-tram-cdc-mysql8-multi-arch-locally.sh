#! /bin/bash -e

export DATABASE=mysql
export DATABASE_VERSION=8-multi-arch
export MODE=binlog-multi-arch
export READER=MySqlReader

if [ "$1" != "--skip-build" ] ; then

  ./gradlew :eventuate-cdc-service:assemble

  ./eventuate-cdc-service/build-docker-multi-arch.sh

fi

docker pull localhost:5002/eventuate-cdc-service:multi-arch-local-build

./scripts/_build-and-test-all-tram-cdc.sh
