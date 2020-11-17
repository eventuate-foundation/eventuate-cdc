#! /bin/bash

set -e

export DATABASE=postgres
export MODE=polling
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling
export READER=PostgresPollingReader

./scripts/_build-and-test-all-tram-cdc.sh
