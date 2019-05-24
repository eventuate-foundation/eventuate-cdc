package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.rabbitmq.producer.EventuateRabbitMQProducer;

import java.util.concurrent.CompletableFuture;

public class EventuateRabbitMQDataProducerWrapper implements DataProducer {

  private EventuateRabbitMQProducer eventuateRabbitMQProducer;

  public EventuateRabbitMQDataProducerWrapper(EventuateRabbitMQProducer eventuateRabbitMQProducer) {
    this.eventuateRabbitMQProducer = eventuateRabbitMQProducer;
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return eventuateRabbitMQProducer.send(topic, key, body);
  }

  @Override
  public void close() {
    eventuateRabbitMQProducer.close();
  }
}
