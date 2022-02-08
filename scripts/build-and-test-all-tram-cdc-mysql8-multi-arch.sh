#! /bin/bash -e

export DATABASE=mysql
export DATABASE_VERSION=8-multi-arch
export MODE=binlog
export READER=MySqlReader

./scripts/_build-and-test-all-tram-cdc.sh
