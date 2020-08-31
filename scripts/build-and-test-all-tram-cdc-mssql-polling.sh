#! /bin/bash

set -e

export DATABASE=mssql
export MODE=polling
export SPRING_PROFILES_ACTIVE=mssql,EventuatePolling

./scripts/_build-and-test-all-tram-cdc.sh
