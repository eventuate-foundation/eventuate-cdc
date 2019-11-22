package io.eventuate.local.common;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CdcDataPublisher<EVENT extends BinLogEvent> {
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  //  protected MeterRegistry meterRegistry;
  protected PublishingStrategy<EVENT> publishingStrategy;
  protected DataProducerFactory dataProducerFactory;
  protected DataProducer producer;
//  protected Counter meterEventsPublished;
//  protected Counter meterEventsDuplicates;
//  protected Counter meterEventsRetries;
//  protected DistributionSummary distributionSummaryEventAge;

  private PublishingFilter publishingFilter;
  private volatile boolean lastMessagePublishingFailed;
  private AtomicLong timeOfLastProcessedEvent = new AtomicLong(0);
  private AtomicInteger totallyProcessedEvents = new AtomicInteger(0);

  public CdcDataPublisher(DataProducerFactory dataProducerFactory,
                          PublishingFilter publishingFilter,
                          PublishingStrategy<EVENT> publishingStrategy,
                          MeterRegistry meterRegistry) {

    this.dataProducerFactory = dataProducerFactory;
    this.publishingStrategy = publishingStrategy;
    this.publishingFilter = publishingFilter;
//    this.meterRegistry = meterRegistry;
//    initMetrics();
  }

  public boolean isLastMessagePublishingFailed() {
    return lastMessagePublishingFailed;
  }

//  private void initMetrics() {
//    if (meterRegistry != null) {
//      distributionSummaryEventAge = meterRegistry.summary("eventuate.cdc.event.age");
//      meterEventsPublished = meterRegistry.counter("eventuate.cdc.events.published");
//      meterEventsDuplicates = meterRegistry.counter("eventuate.cdc.events.duplicates");
//      meterEventsRetries = meterRegistry.counter("eventuate.cdc.events.retries");
//    }
//  }

  public int getTotallyProcessedEventCount() {
    return totallyProcessedEvents.get();
  }

  public long getTimeOfLastPrcessedEvent() {
    return timeOfLastProcessedEvent.get();
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

  public Optional<CompletableFuture<?>> handleEvent(EVENT publishedEvent) throws EventuateLocalPublishingException {

    Objects.requireNonNull(publishedEvent);

    String json = publishingStrategy.toJson(publishedEvent);

    logger.info("Got record: {}", json);

    String aggregateTopic = publishingStrategy.topicFor(publishedEvent);

    if (publishedEvent.getBinlogFileOffset().map(o -> publishingFilter.shouldBePublished(o, aggregateTopic)).orElse(true)) {
      logger.info("sending record: {}", json);

      CompletableFuture<Object> result = new CompletableFuture<>();

      send(publishedEvent, aggregateTopic, json, new AtomicInteger(0), result);

      return Optional.of(result);
    } else {
      logger.debug("Duplicate event {}", publishedEvent);
      return Optional.empty();
    }
  }

  private void send(EVENT publishedEvent, String aggregateTopic, String json, AtomicInteger retries, CompletableFuture<Object> result) {
    producer
            .send(aggregateTopic, publishingStrategy.partitionKeyFor(publishedEvent), json)
            .whenComplete((o, throwable) -> {
              if (throwable != null) {
                if (retries.incrementAndGet() < 5) {
                  send(publishedEvent, aggregateTopic, json, retries, result);
                }
                else {
                  result.completeExceptionally(throwable);
                }
              }
              else {
                result.complete(o);
                timeOfLastProcessedEvent.set(System.nanoTime());
                totallyProcessedEvents.incrementAndGet();
              }
            });
  }
}
