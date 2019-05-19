#! /bin/bash

set -e

export DATABASE=postgres
export MODE=wal

./scripts/_build-and-test-all-tram-cdc.sh
