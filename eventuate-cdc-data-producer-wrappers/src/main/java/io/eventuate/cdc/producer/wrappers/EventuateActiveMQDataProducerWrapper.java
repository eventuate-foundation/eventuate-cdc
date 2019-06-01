package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.activemq.producer.EventuateActiveMQProducer;

import java.util.concurrent.CompletableFuture;

public class EventuateActiveMQDataProducerWrapper implements DataProducer {

  private EventuateActiveMQProducer eventuateActiveMQProducer;

  public EventuateActiveMQDataProducerWrapper(EventuateActiveMQProducer eventuateActiveMQProducer) {
    this.eventuateActiveMQProducer = eventuateActiveMQProducer;
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return eventuateActiveMQProducer.send(topic, key, body);
  }

  @Override
  public void close() {
    eventuateActiveMQProducer.close();
  }
}
