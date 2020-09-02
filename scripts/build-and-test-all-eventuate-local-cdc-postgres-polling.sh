export DATABASE=postgres
export MODE=polling

export SPRING_PROFILES_ACTIVE=postgres,EventuatePolling

./scripts/_build-and-test-all-eventuate-local-cdc.sh