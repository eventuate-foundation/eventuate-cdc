package io.eventuate.cdc.producer.wrappers;

import io.eventuate.messaging.activemq.producer.EventuateActiveMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class EventuateActiveMQDataProducerWrapper implements DataProducer {
  private Logger logger = LoggerFactory.getLogger(getClass());

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
    logger.info("closing EventuateActiveMQDataProducerWrapper");
    eventuateActiveMQProducer.close();
    logger.info("closed EventuateActiveMQDataProducerWrapper");

  }
}
