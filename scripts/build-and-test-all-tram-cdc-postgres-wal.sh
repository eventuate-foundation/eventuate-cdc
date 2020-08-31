#! /bin/bash

set -e

export DATABASE=postgres
export MODE=wal
export SPRING_PROFILES_ACTIVE=postgres,PostgresWal

./scripts/_build-and-test-all-tram-cdc.sh
