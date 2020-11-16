#! /bin/bash

set -e

export DATABASE=mysql
export MODE=binlog
export READER=MySqlReader

./scripts/_build-and-test-all-tram-cdc.sh
