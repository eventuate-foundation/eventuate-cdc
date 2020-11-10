#! /bin/bash

export TERM=dumb

set -e

. ./scripts/set-env.sh

dockermysql="./gradlew mysqlonlyCompose"

${dockermysql}Down
${dockermysql}Up


./gradlew -P testMessageTableColumnReordering=true  :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogMessageTableColumnReorderdingTest
${dockermysql}Down
${dockermysql}Up
./gradlew -P testMessageTableRecreation=true :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogMessageTableRecreationTest

${dockermysql}Down