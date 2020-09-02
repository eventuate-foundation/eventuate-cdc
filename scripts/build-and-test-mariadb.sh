#!/bin/bash -e

export DATABASE=mariadb

./scripts/_build-and-test-mysql.sh :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest :eventuate-local-java-cdc-connector-mysql-binlog:test
