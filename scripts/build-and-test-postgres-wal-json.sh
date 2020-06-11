#! /bin/bash

set -e

export DATABASE=postgres
export MODE=wal
export TEST_MODULE=eventuate-local-java-cdc-connector-postgres-wal
export TEST_CLASS=io.eventuate.local.postgres.wal.PostgresWalBinlogEntryReaderMessageTableTest

./scripts/_build-and-test-json.sh