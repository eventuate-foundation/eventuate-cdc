#! /bin/bash

set -e

export DATABASE=postgres
export MODE=polling

./scripts/_build-and-test-all-tram-cdc.sh
