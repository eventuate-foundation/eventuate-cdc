#! /bin/bash

set -e

export DATABASE=mssql
export MODE=polling
export TEST_MODULE=eventuate-local-java-cdc-connector-polling
export TEST_CLASS=io.eventuate.local.polling.PollingBinlogEntryReaderMessageTableTest
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling

./scripts/_build-and-test-json.sh