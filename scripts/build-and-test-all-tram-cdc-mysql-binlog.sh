#! /bin/bash

set -e

export DATABASE=mysql
export MODE=binlog

./scripts/_build-and-test-all-tram-cdc.sh
