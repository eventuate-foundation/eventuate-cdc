package io.eventuate.cdc.producer.wrappers;

public interface DataProducerFactory {
  DataProducer create();
}