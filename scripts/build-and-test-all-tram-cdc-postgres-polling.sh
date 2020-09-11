#! /bin/bash

set -e

export DATABASE=postgres
export MODE=polling
export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling

./scripts/_build-and-test-all-tram-cdc.sh
