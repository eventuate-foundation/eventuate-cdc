package io.eventuate.local.common;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CdcDataPublisher<EVENT extends BinLogEvent> {
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  protected MeterRegistry meterRegistry;
  protected PublishingStrategy<EVENT> publishingStrategy;
  protected DataProducerFactory dataProducerFactory;
  protected DataProducer producer;

  private PublishingFilter publishingFilter;
  private volatile boolean lastMessagePublishingFailed;

  private final ConcurrentHashMap<String, MessageSender<EVENT>> messageSenders = new ConcurrentHashMap<>();

  public CdcDataPublisher(DataProducerFactory dataProducerFactory,
                          PublishingFilter publishingFilter,
                          PublishingStrategy<EVENT> publishingStrategy,
                          MeterRegistry meterRegistry) {

    this.dataProducerFactory = dataProducerFactory;
    this.publishingStrategy = publishingStrategy;
    this.publishingFilter = publishingFilter;
    this.meterRegistry = meterRegistry;
  }

  public int getTotallyProcessedEventCount() {
    return messageSenders
            .values()
            .stream()
            .map(MessageSender::getEventCount)
            .map(AtomicInteger::get)
            .reduce(0, (a, b) -> a + b);
  }

  public long getTimeOfLastPrcessedEvent() {
    return messageSenders
            .values()
            .stream()
            .map(MessageSender::getLastEventProcessingTime)
            .map(AtomicLong::get)
            .max(Long::compareTo)
            .orElse(0L);
  }

  public boolean isLastMessagePublishingFailed() {
    return lastMessagePublishingFailed;
  }

  public void start() {
    logger.debug("Starting CdcDataPublisher");
    producer = dataProducerFactory.create();
    logger.debug("Starting CdcDataPublisher");
  }

  public void stop() {
    logger.debug("Stopping data producer");
    if (producer != null)
      producer.close();
  }

  public void handleEvent(EVENT publishedEvent) throws EventuateLocalPublishingException {
    Objects.requireNonNull(publishedEvent);

    String topic = publishingStrategy.topicFor(publishedEvent);
    String json = publishingStrategy.toJson(publishedEvent);
    logger.info("Got record: {}", json);

    if (!lastMessagePublishingFailed) {
      Optional<Throwable> lastThrowable = getOrCreateTopicPartitionSender(topic).handleEvent(publishedEvent);

      lastThrowable.ifPresent(throwable -> {
        lastMessagePublishingFailed = true;
        throw new RuntimeException(throwable);
      });
    } else {
      throw new RuntimeException("Message processing failed!");
    }
  }

  private MessageSender<EVENT> getOrCreateTopicPartitionSender(String topic) {
    MessageSender<EVENT> messageSender = messageSenders.get(topic);

    if (messageSender == null) {
      synchronized (messageSenders) {
        messageSender = messageSenders.get(topic);

        if (messageSender == null) {
          messageSender = new MessageSender<>(producer, publishingFilter, publishingStrategy, meterRegistry);
          messageSenders.put(topic, messageSender);
        }

      }
    }

    return messageSender;
  }
}
