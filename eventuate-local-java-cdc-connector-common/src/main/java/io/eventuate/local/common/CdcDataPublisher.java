package io.eventuate.local.common;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
  private long sendTimeAccumulator = 0;

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

  public long getTimeOfLastProcessedEvent() {
    return timeOfLastProcessedEvent.get();
  }

  public long getSendTimeAccumulator() {
    return sendTimeAccumulator;
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

  public CompletableFuture<?> sendMessage(EVENT publishedEvent) throws EventuateLocalPublishingException {

    Objects.requireNonNull(publishedEvent);

    String json = publishingStrategy.toJson(publishedEvent);

    logger.info("Got record: {}", json);

    String aggregateTopic = publishingStrategy.topicFor(publishedEvent);

    CompletableFuture<Object> result = new CompletableFuture<>();

    if (publishedEvent.getBinlogFileOffset().map(o -> publishingFilter.shouldBePublished(o, aggregateTopic)).orElse(true)) {
      logger.info("sending record: {}", json);

      long t = System.nanoTime();
      send(publishedEvent, aggregateTopic, json, 0, result);
      sendTimeAccumulator += System.nanoTime() - t;

      return result;
    } else {
      logger.debug("Duplicate event {}", publishedEvent);
      result.complete(null);
      return result;
    }
  }

  private void send(EVENT publishedEvent, String aggregateTopic, String json, int retries, CompletableFuture<Object> result) {
    producer
            .send(aggregateTopic, publishingStrategy.partitionKeyFor(publishedEvent), json)
            .whenComplete((o, throwable) -> {
              if (throwable != null) {
                if ((retries + 1) < 5) {
                  send(publishedEvent, aggregateTopic, json, retries + 1, result);
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
