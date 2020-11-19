package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class BinlogEntryReader {
  protected Logger logger = LoggerFactory.getLogger(getClass());
  protected MeterRegistry meterRegistry;
  protected List<BinlogEntryHandler> binlogEntryHandlers = new CopyOnWriteArrayList<>();
  protected AtomicBoolean running = new AtomicBoolean(false);
  protected CountDownLatch stopCountDownLatch;
  protected String dataSourceUrl;
  protected DataSource dataSource;
  protected String readerName;
  protected Long outboxId;
  protected CommonCdcMetrics commonCdcMetrics;
  protected volatile Optional<String> processingError = Optional.empty();

  private volatile long lastEventTime = 0;

  protected Optional<Runnable> restartCallback = Optional.empty();

  public BinlogEntryReader(MeterRegistry meterRegistry,
                           String dataSourceUrl,
                           DataSource dataSource,
                           String readerName,
                           Long outboxId) {

    this.meterRegistry = meterRegistry;
    this.dataSourceUrl = dataSourceUrl;
    this.dataSource = dataSource;
    this.readerName = readerName;
    this.outboxId = outboxId;

    commonCdcMetrics = new CommonCdcMetrics(meterRegistry, readerName);
  }

  public abstract CdcProcessingStatusService getCdcProcessingStatusService();

  public Optional<String> getProcessingError() {
    return processingError;
  }

  public String getReaderName() {
    return readerName;
  }

  public Long getOutboxId() {
    return outboxId;
  }

  public long getLastEventTime() {
    return lastEventTime;
  }

  public <EVENT extends BinLogEvent> BinlogEntryHandler addBinlogEntryHandler(EventuateSchema eventuateSchema,
                                                                              String sourceTableName,
                                                                              BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                                                                              Function<EVENT, CompletableFuture<?>> eventPublisher) {
    logger.info("Adding binlog entry handler for schema = {}, table = {}", eventuateSchema.getEventuateDatabaseSchema(), sourceTableName);

    if (eventuateSchema.isEmpty()) {
      throw new IllegalArgumentException("The eventuate schema cannot be empty for the cdc processor.");
    }

    SchemaAndTable schemaAndTable = new SchemaAndTable(eventuateSchema.getEventuateDatabaseSchema(), sourceTableName);

    BinlogEntryHandler binlogEntryHandler =
            new BinlogEntryHandler<>(schemaAndTable, binlogEntryToEventConverter, eventPublisher);

    binlogEntryHandlers.add(binlogEntryHandler);

    logger.info("Added binlog entry handler for schema = {}, table = {}", eventuateSchema.getEventuateDatabaseSchema(), sourceTableName);

    return binlogEntryHandler;
  }

  public void start() {
    commonCdcMetrics.setLeader(true);
  }

  public void stop() {
    stop(true);
  }

  public void stop(boolean removeHandlers) {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    try {
      stopCountDownLatch.await();
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }

    if (removeHandlers) {
      binlogEntryHandlers.clear();
    }
    stopMetrics();
  }

  public void setRestartCallback(Runnable restartCallback) {
    this.restartCallback = Optional.of(restartCallback);
  }

  protected void stopMetrics() {
    commonCdcMetrics.setLeader(false);
  }

  protected void onEventReceived() {
    commonCdcMetrics.onMessageProcessed();
    onActivity();
  }

  protected void onActivity() {
    lastEventTime = System.currentTimeMillis();
  }

  protected void handleProcessingFailException(Throwable e) {
    logger.error("Stopping due to exception", e);
    processingError = Optional.of(e.getMessage());
    stopCountDownLatch.countDown();
    throw new RuntimeException(e);
  }
}
