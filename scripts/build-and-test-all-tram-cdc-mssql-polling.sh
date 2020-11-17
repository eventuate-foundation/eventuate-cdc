#! /bin/bash

set -e

export DATABASE=mssql
export MODE=polling
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling
export READER=MsSqlReader

./scripts/_build-and-test-all-tram-cdc.sh
