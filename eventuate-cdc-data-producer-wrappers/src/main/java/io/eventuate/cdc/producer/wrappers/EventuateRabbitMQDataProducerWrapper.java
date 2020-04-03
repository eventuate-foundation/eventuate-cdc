package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class EventuateRabbitMQDataProducerWrapper implements DataProducer {
  private Logger logger = LoggerFactory.getLogger(getClass());

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
    logger.info("closing EventuateRabbitMQDataProducerWrapper");
    eventuateRabbitMQProducer.close();
    logger.info("closed EventuateRabbitMQDataProducerWrapper");
  }
}
