#! /bin/bash

export TERM=dumb

set -e

. ./scripts/set-env.sh

export USE_DB_ID=true
export USE_JSON_PAYLOAD_AND_HEADERS=true

dockermysql="./gradlew mysqlonlyCompose"
dockermariadb="./gradlew mariadbonlyCompose"
dockerpostgres="./gradlew mssqlonlyCompose"
dockermssql="./gradlew postgresonlyCompose"

${dockermysql}Down
${dockermariadb}Down
${dockermssql}Down
${dockerpostgres}Down

${dockermysql}Up
${dockermssql}Up
${dockerpostgres}Up

echo "TESTING APPLICATION GENERATION ID WITH DATABASE WITH DBID"

./gradlew :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest
${dockermysql}Down
${dockermariadb}Up
./gradlew :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest
./gradlew :eventuate-local-java-cdc-connector-postgres-wal:cleanTest eventuate-local-java-cdc-connector-postgres-wal:test --tests io.eventuate.local.postgres.wal.PostgresWalBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
./gradlew :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
./gradlew :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest

echo "TESTING DATABASE GENERATION ID"

unset SPRING_PROFILES_ACTIVE
export EVENTUATE_OUTBOX_ID=1

./gradlew :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest
${dockermariadb}Down
${dockermysql}Up
./gradlew :eventuate-local-java-cdc-connector-mysql-binlog:cleanTest eventuate-local-java-cdc-connector-mysql-binlog:test --tests io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest
./gradlew :eventuate-local-java-cdc-connector-postgres-wal:cleanTest eventuate-local-java-cdc-connector-postgres-wal:test --tests io.eventuate.local.postgres.wal.PostgresWalBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
./gradlew :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
./gradlew :eventuate-local-java-cdc-connector-polling:cleanTest eventuate-local-java-cdc-connector-polling:test --tests io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest

${dockermysql}Down
${dockermssql}Down
${dockerpostgres}Down