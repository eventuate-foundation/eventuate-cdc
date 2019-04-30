#! /bin/bash

set -e

export DATABASE=mssql
export MODE=polling

./scripts/_build-and-test-all-tram-cdc.sh
