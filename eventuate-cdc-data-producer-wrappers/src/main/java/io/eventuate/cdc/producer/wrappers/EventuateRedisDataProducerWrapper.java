package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.redis.producer.EventuateRedisProducer;

import java.util.concurrent.CompletableFuture;

public class EventuateRedisDataProducerWrapper implements DataProducer {

  private EventuateRedisProducer eventuateRedisProducer;

  public EventuateRedisDataProducerWrapper(EventuateRedisProducer eventuateRedisProducer) {
    this.eventuateRedisProducer = eventuateRedisProducer;
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return eventuateRedisProducer.send(topic, key, body);
  }

  @Override
  public void close() {
    eventuateRedisProducer.close();
  }
}
