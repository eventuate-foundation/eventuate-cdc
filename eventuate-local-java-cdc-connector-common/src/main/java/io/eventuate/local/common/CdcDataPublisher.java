package io.eventuate.local.common;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CdcDataPublisher<EVENT extends BinLogEvent> {
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  protected MeterRegistry meterRegistry;
  protected PublishingStrategy<EVENT> publishingStrategy;
  protected DataProducerFactory dataProducerFactory;
  protected DataProducer producer;
  protected Counter meterEventsPublished;
  protected Counter meterEventsDuplicates;
  protected DistributionSummary distributionSummaryEventAge;

  private PublishingFilter publishingFilter;
  private volatile boolean lastMessagePublishingFailed;
  private AtomicLong timeOfLastProcessedEvent = new AtomicLong(0);
  private AtomicInteger totallyProcessedEvents = new AtomicInteger(0);
  private long sendTimeAccumulator = 0;

  public CdcDataPublisher(DataProducerFactory dataProducerFactory,
                          PublishingFilter publishingFilter,
                          PublishingStrategy<EVENT> publishingStrategy,
                          MeterRegistry meterRegistry) {

    this.dataProducerFactory = dataProducerFactory;
    this.publishingStrategy = publishingStrategy;
    this.publishingFilter = publishingFilter;
    this.meterRegistry = meterRegistry;
    initMetrics();
  }

  public boolean isLastMessagePublishingFailed() {
    return lastMessagePublishingFailed;
  }

  private void initMetrics() {
    if (meterRegistry != null) {
      distributionSummaryEventAge = meterRegistry.summary("eventuate.cdc.event.age");
      meterEventsPublished = meterRegistry.counter("eventuate.cdc.events.sent");
      meterEventsDuplicates = meterRegistry.counter("eventuate.cdc.events.duplicates");
    }
  }

  public int getTotallyProcessedEventCount() {
    return totallyProcessedEvents.get();
  }

  public long getTimeOfLastProcessedEvent() {
    return timeOfLastProcessedEvent.get();
  }

  public long getSendTimeAccumulator() {
    return sendTimeAccumulator;
  }

  public void start() {
    logger.info("Starting CdcDataPublisher");
    producer = dataProducerFactory.create();
    logger.info("Started CdcDataPublisher");
  }

  public void stop() {
    logger.info("Stopping CdcDataPublisher");
    if (producer != null)
      producer.close();
    logger.info("Stopped CdcDataPublisher");
  }

  public CompletableFuture<?> sendMessage(EVENT publishedEvent) throws EventuateLocalPublishingException {

    Objects.requireNonNull(publishedEvent);

    String json = publishingStrategy.toJson(publishedEvent);

    logger.debug("Got record: {}", json);

    String aggregateTopic = publishingStrategy.topicFor(publishedEvent);

    CompletableFuture<Object> result = new CompletableFuture<>();

    if (publishedEvent.getBinlogFileOffset().map(o -> publishingFilter.shouldBePublished(o, aggregateTopic)).orElse(true)) {
      logger.debug("sending record: {}", json);

      long t = System.nanoTime();
      send(publishedEvent, aggregateTopic, json, result);
      meterEventsPublished.increment();
      publishingStrategy.getCreateTime(publishedEvent).ifPresent(time -> distributionSummaryEventAge.record(System.currentTimeMillis() - time));
      sendTimeAccumulator += System.nanoTime() - t;

      return result;
    } else {
      logger.debug("Duplicate event {}", publishedEvent);
      meterEventsDuplicates.increment();
      result.complete(null);
      return result;
    }
  }

  private void send(EVENT publishedEvent, String aggregateTopic, String json, CompletableFuture<Object> result) {
    producer
            .send(aggregateTopic, publishingStrategy.partitionKeyFor(publishedEvent), json)
            .whenComplete((o, throwable) -> {
              if (throwable != null) {
                result.completeExceptionally(throwable);
              }
              else {
                result.complete(o);
                timeOfLastProcessedEvent.set(System.nanoTime());
                totallyProcessedEvents.incrementAndGet();
              }
            });
  }
}
