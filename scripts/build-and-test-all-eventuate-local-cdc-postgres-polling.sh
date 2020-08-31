export database=postgres
export mode=polling

export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling

./scripts/_build-and-test-all-eventuate-local-cdc.sh