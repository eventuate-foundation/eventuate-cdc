package io.eventuate.local.postgres.wal;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.SchemaAndTable;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.*;
import io.eventuate.local.db.log.common.DbLogClient;
import io.micrometer.core.instrument.MeterRegistry;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PostgresWalClient extends DbLogClient {
  private PostgresWalBinlogEntryExtractor postgresWalBinlogEntryExtractor;
  private int walIntervalInMilliseconds;
  private int connectionTimeoutInMilliseconds;
  private int maxAttemptsForBinlogConnection;
  private Connection connection;
  private PGReplicationStream stream;
  private int replicationStatusIntervalInMilliseconds;
  private String replicationSlotName;
  private final PostgresWalCdcProcessingStatusService postgresWalCdcProcessingStatusService;
  private OffsetProcessor<LogSequenceNumber> offsetProcessor;

  public PostgresWalClient(MeterRegistry meterRegistry,
                           String url,
                           String user,
                           String password,
                           int walIntervalInMilliseconds,
                           int connectionTimeoutInMilliseconds,
                           int maxAttemptsForBinlogConnection,
                           int replicationStatusIntervalInMilliseconds,
                           String replicationSlotName,
                           DataSource dataSource,
                           String readerName,
                           long replicationLagMeasuringIntervalInMilliseconds,
                           int monitoringRetryIntervalInMilliseconds,
                           int monitoringRetryAttempts,
                           String additionalServiceReplicationSlotName,
                           long waitForOffsetSyncTimeoutInMilliseconds,
                           EventuateSchema monitoringSchema,
                           Long outboxId) {

    super(meterRegistry,
            user,
            password,
            url,
            dataSource,
            readerName,
            replicationLagMeasuringIntervalInMilliseconds,
            monitoringRetryIntervalInMilliseconds,
            monitoringRetryAttempts,
            monitoringSchema,
            outboxId);

    this.walIntervalInMilliseconds = walIntervalInMilliseconds;
    this.connectionTimeoutInMilliseconds = connectionTimeoutInMilliseconds;
    this.maxAttemptsForBinlogConnection = maxAttemptsForBinlogConnection;
    this.replicationStatusIntervalInMilliseconds = replicationStatusIntervalInMilliseconds;
    this.replicationSlotName = replicationSlotName;
    this.postgresWalBinlogEntryExtractor = new PostgresWalBinlogEntryExtractor();

    postgresWalCdcProcessingStatusService = new PostgresWalCdcProcessingStatusService(dataSource,
            additionalServiceReplicationSlotName,
            waitForOffsetSyncTimeoutInMilliseconds);
  }

  @Override
  public CdcProcessingStatusService getCdcProcessingStatusService() {
    return postgresWalCdcProcessingStatusService;
  }


  @Override
  public void start() {
    logger.info("Starting PostgresWalClient");
    super.start();

    stopCountDownLatch = new CountDownLatch(1);
    running.set(true);

    connectWithRetriesOnFail();
    logger.info("PostgresWalClient finished processing");
  }

  private void connectWithRetriesOnFail() {
    for (int i = 1; running.get(); i++) {
      try {
        logger.info("trying to connect to postgres wal");
        connectAndRun();
        break;
      } catch (SQLException e) {
        onDisconnected();
        logger.error("connection to postgres wal failed");
        if (i == maxAttemptsForBinlogConnection) {
          handleProcessingFailException(e);
        }
        try {
          Thread.sleep(connectionTimeoutInMilliseconds);
        } catch (InterruptedException ex) {
          handleProcessingFailException(e);
        }
      } catch (Exception e) {
        handleProcessingFailException(e);
      }
    }
    stopCountDownLatch.countDown();
  }

  private void connectAndRun()
          throws SQLException {

    Properties props = new Properties();
    PGProperty.USER.set(props, dbUserName);
    PGProperty.PASSWORD.set(props, dbPassword);
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
    PGProperty.REPLICATION.set(props, "database");
    PGProperty.PREFER_QUERY_MODE.set(props, "simple");

    connection = DriverManager.getConnection(dataSourceUrl, props);

    PGConnection replConnection = connection.unwrap(PGConnection.class);

    stream = replConnection.getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(replicationSlotName)
            .withSlotOption("include-xids", false)
            .withSlotOption("write-in-chunks", true)
            .withStatusInterval(replicationStatusIntervalInMilliseconds, TimeUnit.MILLISECONDS)
            .start();

    offsetProcessor = new OffsetProcessor<>(logSequenceNumber -> {
      stream.setAppliedLSN(stream.getLastReceiveLSN());
      stream.setFlushedLSN(stream.getLastReceiveLSN());
      try {
        stream.forceUpdateStatus();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }, this::handleProcessingFailException);

    onConnected();

    logger.info("connection to postgres wal succeed");

    StringBuilder messageBuilder = new StringBuilder();

    while (running.get()) {
      ByteBuffer messageBuffer = stream.readPending();

      if (messageBuffer == null) {
        saveOffsetOfLastProcessedEvent();
        logger.debug("Got empty message, sleeping");
        try {
          TimeUnit.MILLISECONDS.sleep(walIntervalInMilliseconds);
        } catch (InterruptedException e) {
          handleProcessingFailException(e);
        }
        continue;
      }

      String messagePart = extractStringFromBuffer(messageBuffer);

      messageBuilder.append(messagePart);

      if (!"]}".equals(messagePart)) {
        continue;
      }

      dbLogMetrics.onBinlogEntryProcessed();

      String messageString = messageBuilder.toString();
      messageBuilder.setLength(0);

      logger.debug("Got message: {}", messageString);

      PostgresWalMessage postgresWalMessage = JSonMapper.fromJson(messageString, PostgresWalMessage.class);

      checkMonitoringChange(postgresWalMessage);

      LogSequenceNumber lastReceivedLSN = stream.getLastReceiveLSN();

      logger.debug("received offset: {} == {}", lastReceivedLSN, lastReceivedLSN.asLong());

      List<BinlogEntryWithSchemaAndTable> inserts = Arrays
              .stream(postgresWalMessage.getChange())
              .filter(change -> change.getKind().equals("insert"))
              .map(change -> BinlogEntryWithSchemaAndTable.make(postgresWalBinlogEntryExtractor, change))
              .collect(Collectors.toList());

      binlogEntryHandlers.forEach(handler ->
              inserts
                  .stream()
                  .filter(entry -> handler.isFor(entry.getSchemaAndTable()))
                  .map(BinlogEntryWithSchemaAndTable::getBinlogEntry)
                  .forEach(e -> handleBinlogEntry(e, handler)));
      saveOffsetOfLastProcessedEvent();
    }

    stopCountDownLatch.countDown();
  }

  private void handleBinlogEntry(BinlogEntry entry, BinlogEntryHandler handler) {
    LogSequenceNumber logSequenceNumber = stream.getLastReceiveLSN();

    CompletableFuture<Optional<LogSequenceNumber>> futureOffset = new CompletableFuture<>();

    CompletableFuture<?> future = null;

    try {
      future = handler.publish(entry);
    } catch (Exception e) {
      handleProcessingFailException(e);
    }

    future.whenComplete((o, throwable) -> {
      if (throwable == null) {
        futureOffset.complete(Optional.of(logSequenceNumber));
      }
      else {
        futureOffset.completeExceptionally(throwable);
        handleProcessingFailException(throwable);
      }
    });

    offsetProcessor.saveOffset(futureOffset);

    onEventReceived();
  }

  @Override
  public void stop(boolean removeHandlers) {
    logger.info("Stopping PostgresWalClient");
    super.stop(removeHandlers);

    try {
      stream.close();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }

    try {
      connection.close();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
    logger.info("Stopped PostgresWalClient");
  }

  private void checkMonitoringChange(PostgresWalMessage postgresWalMessage) {
    Optional<PostgresWalChange> monitoringChange = Arrays
            .stream(postgresWalMessage.getChange())
            .filter(change -> {
              String changeSchema = change.getSchema();
              String changeTable = change.getTable();
              return cdcMonitoringDao.isMonitoringTableChange(changeSchema, changeTable);
            })
            .findAny();

    monitoringChange.ifPresent(change -> {
      int index = Arrays.asList(change.getColumnnames()).indexOf("last_time");
      dbLogMetrics.onLagMeasurementEventReceived(Long.parseLong(change.getColumnvalues()[index]));
      onEventReceived();
    });
  }


  private String extractStringFromBuffer(ByteBuffer byteBuffer) {
    int offset = byteBuffer.arrayOffset();
    byte[] source = byteBuffer.array();
    int length = source.length - offset;

    return new String(source, offset, length);
  }

  private void saveOffsetOfLastProcessedEvent() {
    if (postgresWalCdcProcessingStatusService != null) {
      postgresWalCdcProcessingStatusService.saveEndingOffsetOfLastProcessedEvent(stream.getLastReceiveLSN().asLong());
    }
  }

  private static class BinlogEntryWithSchemaAndTable {
    private BinlogEntry binlogEntry;
    private SchemaAndTable schemaAndTable;

    public BinlogEntryWithSchemaAndTable(BinlogEntry binlogEntry, SchemaAndTable schemaAndTable) {
      this.binlogEntry = binlogEntry;
      this.schemaAndTable = schemaAndTable;
    }

    public BinlogEntry getBinlogEntry() {
      return binlogEntry;
    }

    public SchemaAndTable getSchemaAndTable() {
      return schemaAndTable;
    }

    public static BinlogEntryWithSchemaAndTable make(PostgresWalBinlogEntryExtractor extractor,
                                                     PostgresWalChange change) {

      BinlogEntry binlogEntry = extractor.extract(change);
      SchemaAndTable schemaAndTable = new SchemaAndTable(change.getSchema(), change.getTable());
      return new BinlogEntryWithSchemaAndTable(binlogEntry, schemaAndTable);
    }
  }
}
