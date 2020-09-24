#! /bin/bash

set -e

export DATABASE=postgres
export MODE=wal
export TEST_MODULE=eventuate-local-java-cdc-connector-postgres-wal
export TEST_CLASS=io.eventuate.local.postgres.wal.PostgresWalBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=postgres,PostgresWal

./scripts/_build-and-test-database-id.sh