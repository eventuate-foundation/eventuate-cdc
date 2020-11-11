#! /bin/bash

export TERM=dumb

set -e

. ./scripts/set-env.sh

dockermysql="./gradlew mysqlonlyCompose"
dockermariadb="./gradlew mariadbonlyCompose"
dockerpostgres="./gradlew postgresonlyCompose"
dockermssql="./gradlew mssqlonlyCompose"

function testMysql() {
  ./gradlew :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest
}

function testPolling() {
  ./gradlew :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest
}

function testPostgresWal() {
  ./gradlew :eventuate-local-java-cdc-connector-postgres-wal:cleanTest eventuate-local-java-cdc-connector-postgres-wal:test --tests io.eventuate.local.postgres.wal.PostgresWalBinlogEntryReaderMessageTableTest
}

function testMysqlMessageColumnsReordering() {
  ./gradlew -P testMessageTableColumnReordering=true  :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogMessageTableColumnReorderdingTest
}

function testMysqlMessageTableRecreation() {
  ./gradlew -P testMessageTableRecreation=true :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogMessageTableRecreationTest
}

function testPollingMessageColumnsReordering() {
  ./gradlew -P testMessageTableColumnReordering=true  :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingMessageTableColumnReorderdingTest
}

function testPollingMessageTableRecreation() {
  ./gradlew -P testMessageTableRecreation=true :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingMessageTableRecreationTest
}

function testPostgresWalMessageColumnsReordering() {
  ./gradlew -P testMessageTableColumnReordering=true  :eventuate-local-java-cdc-connector-postgres-wal:cleanTest eventuate-local-java-cdc-connector-postgres-wal:test --tests  io.eventuate.local.postgres.wal.PostgresWalMessageTableColumnReorderdingTest
}

function testPostgresWalMessageTableRecreation() {
  ./gradlew -P testMessageTableRecreation=true :eventuate-local-java-cdc-connector-postgres-wal:cleanTest eventuate-local-java-cdc-connector-postgres-wal:test --tests io.eventuate.local.postgres.wal.PostgresWalMessageTableRecreationTest
}

function startContainer() {
  ./gradlew ${1}onlyComposeUp
}

function stopContainer() {
  ./gradlew ${1}onlyComposeDown
}

function restartContainer() {
  stopContainer $1
  startContainer $1
}

stopContainer mysql
stopContainer mariadb
stopContainer mssql
stopContainer postgres


echo "TESTING MESSAGE TABLE SCHEMA MIGRATION"


startContainer mysql
testMysqlMessageColumnsReordering
restartContainer mysql
testMysqlMessageTableRecreation
stopContainer mysql

startContainer mssql
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
testPollingMessageColumnsReordering
restartContainer mssql
testPollingMessageTableRecreation
stopContainer mssql
unset SPRING_PROFILES_ACTIVE

startContainer postgres
testPostgresWalMessageColumnsReordering
restartContainer postgres
testPostgresWalMessageTableRecreation
stopContainer postgres

export USE_DB_ID=true
export USE_JSON_PAYLOAD_AND_HEADERS=true


echo "TESTING APPLICATION GENERATION ID WITH DATABASE WITH DBID"


startContainer mysql
testMysql
stopContainer mysql

startContainer mariadb
testMysql

startContainer postgres
testPostgresWal

export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
testPolling

startContainer mssql
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
testPolling


echo "TESTING DATABASE GENERATION ID"


unset SPRING_PROFILES_ACTIVE
export EVENTUATE_OUTBOX_ID=1

testMysql
stopContainer mariadb

startContainer mysql
testMysql

testPostgresWal

export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
testPolling

export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
testPolling


stopContainer mysql
stopContainer mssql
stopContainer postgres
