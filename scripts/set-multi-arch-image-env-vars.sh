export MULTI_ARCH_TAG=test-build-${CIRCLE_SHA1?}
export BUILDX_PUSH_OPTIONS=--push

export EVENTUATE_CDC_SERVICE_MULTI_ARCH_IMAGE=eventuateio/eventuate-cdc-service:$MULTI_ARCH_TAG
