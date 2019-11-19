package io.eventuate.local.common;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MessageSender<EVENT extends BinLogEvent> {
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  private final Object lock = new Object();

  private DataProducer producer;
  private PublishingFilter publishingFilter;
  private PublishingStrategy<EVENT> publishingStrategy;
  private MeterRegistry meterRegistry;

  private Counter meterEventsPublished;
  private Counter meterEventsDuplicates;
  private Counter meterEventsRetries;
  private DistributionSummary distributionSummaryEventAge;

  private MessageSenderState state = MessageSenderState.IDLE;
  private LinkedList<EVENT> eventQueue = new LinkedList<>();
  private Optional<Throwable> lastThrowable = Optional.empty();

  private AtomicInteger eventCount = new AtomicInteger(0);
  private AtomicLong lastEventProcessingTime = new AtomicLong(0);

  public MessageSender(DataProducer producer,
                       PublishingFilter publishingFilter,
                       PublishingStrategy<EVENT> publishingStrategy,
                       MeterRegistry meterRegistry) {

    this.producer = producer;
    this.publishingFilter = publishingFilter;
    this.publishingStrategy = publishingStrategy;
    this.meterRegistry = meterRegistry;

    initMetrics();
  }

  public AtomicInteger getEventCount() {
    return eventCount;
  }

  public AtomicLong getLastEventProcessingTime() {
    return lastEventProcessingTime;
  }

  public Optional<Throwable> handleEvent(EVENT publishedEvent) {
    synchronized (lock) {
      switch (state) {
        case IDLE:
          state = MessageSenderState.SENDING;
          sendEvent(publishedEvent);
        break;

        case SENDING:
          eventQueue.add(publishedEvent);
        break;

        case ERROR:
          //nothing to do
        break;
      }
    }

    return lastThrowable;
  }

  private void handleNextEvent(Void aVoid, Throwable throwable) {
    synchronized (lock) {
      if (throwable != null) {
        state = MessageSenderState.ERROR;
        lastThrowable = Optional.of(throwable);
        return;
      }

      EVENT event = eventQueue.poll();
      if (event == null) state = MessageSenderState.IDLE;
      else sendEvent(event);
    }
  }

  private void sendEvent(EVENT publishedEvent) {
    CompletableFuture
            .runAsync(() -> handleEventSending(publishedEvent))
            .whenComplete(this::handleNextEvent);
  }

  private void handleEventSending(EVENT publishedEvent) {

    String aggregateTopic = publishingStrategy.topicFor(publishedEvent);
    String partition = publishingStrategy.partitionKeyFor(publishedEvent);
    String json = publishingStrategy.toJson(publishedEvent);

    Exception lastException = null;

    for (int i = 0; i < 5; i++) {
      try {
        if (publishedEvent.getBinlogFileOffset().map(o -> publishingFilter.shouldBePublished(o, aggregateTopic)).orElse(true)) {
          logger.info("sending record: {}", json);
          producer.send(aggregateTopic, partition, json).get(10, TimeUnit.SECONDS);

          eventCount.incrementAndGet();
          lastEventProcessingTime.set(System.nanoTime());

          logger.info("record sent: {}", json);

          publishingStrategy.getCreateTime(publishedEvent).ifPresent(time -> distributionSummaryEventAge.record(System.currentTimeMillis() - time));
          meterEventsPublished.increment();
        } else {
          logger.debug("Duplicate event {}", publishedEvent);
          meterEventsDuplicates.increment();
        }
        return;
      } catch (Exception e) {

        logger.warn("error publishing to " + aggregateTopic, e);
        meterEventsRetries.increment();
        lastException = e;

        try {
          Thread.sleep((int) Math.pow(2, i) * 1000);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }
    throw new EventuateLocalPublishingException("error publishing to " + aggregateTopic, lastException);
  }

  private void initMetrics() {
    distributionSummaryEventAge = meterRegistry.summary("eventuate.cdc.event.age");
    meterEventsPublished = meterRegistry.counter("eventuate.cdc.events.published");
    meterEventsDuplicates = meterRegistry.counter("eventuate.cdc.events.duplicates");
    meterEventsRetries = meterRegistry.counter("eventuate.cdc.events.retries");
  }

}
