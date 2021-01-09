package io.eventuate.local.polling;

import com.google.common.collect.ImmutableMap;
import io.eventuate.common.common.spring.jdbc.EventuateSpringJdbcStatementExecutor;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.EventuateJdbcStatementExecutor;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.SchemaAndTable;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import io.eventuate.local.common.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PollingDao extends BinlogEntryReader {
  private static final String PUBLISHED_FIELD = "published";

  private DataSource dataSource;
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  private EventuateJdbcStatementExecutor eventuateJdbcStatementExecutor;
  private int maxEventsPerPolling;
  private int maxAttemptsForPolling;
  private int pollingRetryIntervalInMilliseconds;
  private int pollingIntervalInMilliseconds;
  private Map<SchemaAndTable, String> pkFields = new HashMap<>();
  private EventuateSqlDialect eventuateSqlDialect;
  private final PollingProcessingStatusService pollingProcessingStatusService;

  public PollingDao(MeterRegistry meterRegistry,
                    String dataSourceUrl,
                    DataSource dataSource,
                    int maxEventsPerPolling,
                    int maxAttemptsForPolling,
                    int pollingRetryIntervalInMilliseconds,
                    int pollingIntervalInMilliseconds,
                    String readerName,
                    EventuateSqlDialect eventuateSqlDialect,
                    Long outboxId) {

    super(meterRegistry,
            dataSourceUrl,
            dataSource,
            readerName,
            outboxId);

    if (maxEventsPerPolling <= 0) {
      throw new IllegalArgumentException("Max events per polling parameter should be greater than 0.");
    }

    this.dataSource = dataSource;
    this.pollingIntervalInMilliseconds = pollingIntervalInMilliseconds;
    this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    this.eventuateJdbcStatementExecutor = new EventuateSpringJdbcStatementExecutor(new JdbcTemplate(dataSource));
    this.maxEventsPerPolling = maxEventsPerPolling;
    this.maxAttemptsForPolling = maxAttemptsForPolling;
    this.pollingRetryIntervalInMilliseconds = pollingRetryIntervalInMilliseconds;
    this.eventuateSqlDialect = eventuateSqlDialect;

    pollingProcessingStatusService = new PollingProcessingStatusService(dataSource, PUBLISHED_FIELD, eventuateSqlDialect);
  }

  @Override
  public CdcProcessingStatusService getCdcProcessingStatusService() {
    return pollingProcessingStatusService;
  }

  @Override
  public <EVENT extends BinLogEvent> BinlogEntryHandler addBinlogEntryHandler(EventuateSchema eventuateSchema,
                                                                              String sourceTableName,
                                                                              BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                                                                              Function<EVENT, CompletableFuture<?>> eventPublisher) {
    BinlogEntryHandler binlogEntryHandler = super.addBinlogEntryHandler(eventuateSchema, sourceTableName, binlogEntryToEventConverter, eventPublisher);
    pollingProcessingStatusService.addTable(binlogEntryHandler.getQualifiedTable());
    return binlogEntryHandler;
  }

  @Override
  public void start() {
    logger.info("Starting PollingDao");
    super.start();

    stopCountDownLatch = new CountDownLatch(1);
    running.set(true);

    while (running.get()) {
      int processedEvents = 0;
      try {
        processedEvents = binlogEntryHandlers.stream().map(this::processEvents).reduce(0, (a, b) -> a + b);
      } catch (Exception e) {
        handleProcessingFailException(e);
      }

      try {
        if (processedEvents == 0) {
          Thread.sleep(pollingIntervalInMilliseconds);
        }
      } catch (InterruptedException e) {
        handleProcessingFailException(e);
      }
    }

    stopCountDownLatch.countDown();
    logger.info("PollingDao finished processing");
  }

  public int processEvents(BinlogEntryHandler handler) {

    String pk = getPrimaryKey(handler);

    String findEventsQuery = eventuateSqlDialect.addLimitToSql(String.format("SELECT * FROM %s WHERE %s = 0 ORDER BY %s ASC",
            handler.getQualifiedTable(), PUBLISHED_FIELD, pk), ":limit");

    SqlRowSet sqlRowSet = DaoUtils.handleConnectionLost(maxAttemptsForPolling,
            pollingRetryIntervalInMilliseconds,
            () -> namedParameterJdbcTemplate.queryForRowSet(findEventsQuery, ImmutableMap.of("limit", maxEventsPerPolling)),
            this::onInterrupted,
            running);
    List<CompletableFuture<Object>> ids = new ArrayList<>();

    while (sqlRowSet.next()) {
      Object id = sqlRowSet.getObject(pk);
      ids.add(handleEvent(id, handler, sqlRowSet));
      onEventReceived();
    }

    if (!ids.isEmpty()) {
      markEventsAsProcessed(ids, pk, handler);
    }

    onActivity();

    return ids.size();
  }

  private void markEventsAsProcessed(List<CompletableFuture<Object>> eventIds, String pk, BinlogEntryHandler handler) {
    List<Object> ids = eventIds
            .stream()
            .map(this::extractId)
            .collect(Collectors.toList());

    String markEventsAsReadQuery = String.format("UPDATE %s SET %s = 1 WHERE %s in (:ids)",
            handler.getQualifiedTable(), PUBLISHED_FIELD, pk);

    DaoUtils.handleConnectionLost(maxAttemptsForPolling,
            pollingRetryIntervalInMilliseconds,
            () -> namedParameterJdbcTemplate.update(markEventsAsReadQuery, ImmutableMap.of("ids", ids)),
            this::onInterrupted,
            running);
  }

  private Object extractId(CompletableFuture<Object> id) {
    try {
      return id.get();
    } catch (Exception e) {
      handleProcessingFailException(e);
    }
    return null;
  }

  private CompletableFuture<Object> handleEvent(Object id, BinlogEntryHandler handler, SqlRowSet sqlRowSet) {
    SchemaAndTable schemaAndTable = handler.getSchemaAndTable();

    CompletableFuture<?> future = null;

    try {
      future = handler.publish(new BinlogEntry() {
        @Override
        public Object getColumn(String name) {
          return sqlRowSet.getObject(name);
        }

        @Override
        public BinlogFileOffset getBinlogFileOffset() {
          return null;
        }

        @Override
        public String getJsonColumn(String name) {
          return  eventuateSqlDialect
                  .jsonColumnToString(sqlRowSet.getObject(name),
                          new EventuateSchema(schemaAndTable.getSchema()),
                          schemaAndTable.getTableName(),
                          name,
                          eventuateJdbcStatementExecutor);
        }
      });
    } catch (Exception e) {
      handleProcessingFailException(e);
    }

    return future.thenApply(o -> id);
  }

  private String getPrimaryKey(BinlogEntryHandler handler) {
    SchemaAndTable schemaAndTable = handler.getSchemaAndTable();

    if (pkFields.containsKey(schemaAndTable)) {
      return pkFields.get(schemaAndTable);
    }

    String pk = DaoUtils.handleConnectionLost(maxAttemptsForPolling,
            pollingRetryIntervalInMilliseconds,
            () -> eventuateSqlDialect.getPrimaryKeyColumn(dataSource, dataSourceUrl, schemaAndTable),
            this::onInterrupted,
            running);

    pkFields.put(schemaAndTable, pk);

    return pk;
  }

  private void onInterrupted() {
    running.set(false);
    stopCountDownLatch.countDown();
  }
}
