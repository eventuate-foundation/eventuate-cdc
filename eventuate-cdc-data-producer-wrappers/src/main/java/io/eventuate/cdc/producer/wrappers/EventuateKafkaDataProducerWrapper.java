package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;

import java.util.concurrent.CompletableFuture;

public class EventuateKafkaDataProducerWrapper implements DataProducer {

  private EventuateKafkaProducer eventuateKafkaProducer;

  public EventuateKafkaDataProducerWrapper(EventuateKafkaProducer eventuateKafkaProducer) {
    this.eventuateKafkaProducer = eventuateKafkaProducer;
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return eventuateKafkaProducer.send(topic, key, body);
  }

  @Override
  public void close() {
    eventuateKafkaProducer.close();
  }
}
