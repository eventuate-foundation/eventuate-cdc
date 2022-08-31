#! /bin/bash

set -e

export DATABASE=mysql
export DATABASE_VERSION=8
export MODE=polling
export SPRING_PROFILES_ACTIVE=EventuatePolling
export READER=MySqlReader


./scripts/_build-and-test-all-tram-cdc.sh
