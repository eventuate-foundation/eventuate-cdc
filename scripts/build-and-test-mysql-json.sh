#! /bin/bash

set -e

export DATABASE=mysql
export MODE=binlog
export TEST_MODULE=eventuate-local-java-cdc-connector-mysql-binlog
export TEST_CLASS=io.eventuate.local.mysql.binlog.MySqlBinlogEntryReaderMessageTableTest

./scripts/_build-and-test-json.sh