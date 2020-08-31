export database=postgres
export mode=wal

export SPRING_PROFILES_ACTIVE=postgres,PostgresWal

./scripts/_build-and-test-all-eventuate-local-cdc.sh