package io.eventuate.local.mysql.binlog;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.google.common.collect.ImmutableSet;
import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.*;
import io.eventuate.local.db.log.common.DbLogMetrics;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MySqlBinaryLogClientForPerformanceTesting {

  private static final Set<EventType> SUPPORTED_EVENTS = ImmutableSet.of(EventType.TABLE_MAP,
          EventType.ROTATE,
          EventType.WRITE_ROWS,
          EventType.EXT_WRITE_ROWS,
          EventType.UPDATE_ROWS,
          EventType.EXT_UPDATE_ROWS);

  private Long uniqueId;
  private BinaryLogClient client;
  private final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();
  private String binlogFilename;
  private MySqlBinlogEntryExtractor extractor;
  private int connectionTimeoutInMilliseconds;
  private int rowsToSkip;
  private List<BinlogEntryHandler> binlogEntryHandlers = new CopyOnWriteArrayList<>();

  private Optional<Exception> publishingException = Optional.empty();
  private CountDownLatch stopCountDownLatch;
  private AtomicBoolean running = new AtomicBoolean(false);
  private String dbUserName;
  private String dbPassword;
  private String host;
  private int port;
  private Long eventProcessingStartTime;

  public MySqlBinaryLogClientForPerformanceTesting(String dbUserName,
                                                   String dbPassword,
                                                   String dataSourceUrl,
                                                   DataSource dataSource,
                                                   Long uniqueId,
                                                   int connectionTimeoutInMilliseconds) {

    this.dbUserName = dbUserName;
    this.dbPassword = dbPassword;

    JdbcUrl jdbcUrl = JdbcUrlParser.parse(dataSourceUrl);
    host = jdbcUrl.getHost();
    port = jdbcUrl.getPort();

    this.extractor = new MySqlBinlogEntryExtractor(dataSource);
    this.uniqueId = uniqueId;
    this.connectionTimeoutInMilliseconds = connectionTimeoutInMilliseconds;
  }

  public long getEventProcessingStartTime() {
    return eventProcessingStartTime;
  }

  public <EVENT extends BinLogEvent> BinlogEntryHandler addBinlogEntryHandler(EventuateSchema eventuateSchema,
                                                                              String sourceTableName,
                                                                              BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                                                                              CdcDataPublisher<EVENT> dataPublisher) {
    if (eventuateSchema.isEmpty()) {
      throw new IllegalArgumentException("The eventuate schema cannot be empty for the cdc processor.");
    }

    SchemaAndTable schemaAndTable = new SchemaAndTable(eventuateSchema.getEventuateDatabaseSchema(), sourceTableName);

    BinlogEntryHandler binlogEntryHandler =
            new BinlogEntryHandler<>(schemaAndTable, binlogEntryToEventConverter, dataPublisher);

    binlogEntryHandlers.add(binlogEntryHandler);

    return binlogEntryHandler;
  }


  public void start() {

    stopCountDownLatch = new CountDownLatch(1);
    running.set(true);
    publishingException = Optional.empty();

    client = new BinaryLogClient(host, port, dbUserName, dbPassword);
    client.setServerId(uniqueId);
    client.setKeepAliveInterval(5 * 1000);


    BinlogFileOffset bfo = new BinlogFileOffset("", 4L);
    rowsToSkip = bfo.getRowsToSkip();

    client.setBinlogFilename(bfo.getBinlogFilename());
    client.setBinlogPosition(bfo.getOffset());

    client.setEventDeserializer(getEventDeserializer());
    client.registerEventListener(this::handleBinlogEventWithErrorHandling);

    connectWithRetriesOnFail();

    try {
      stopCountDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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
    }

    if (removeHandlers) {
      binlogEntryHandlers.clear();
    }
  }

  private void handleBinlogEventWithErrorHandling(Event event) {
    if (publishingException.isPresent()) {
      return;
    }

    try {
      handleBinlogEvent(event);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private void handleBinlogEvent(Event event) {
    switch (event.getHeader().getEventType()) {
      case TABLE_MAP: {
        TableMapEventData tableMapEvent = event.getData();

        SchemaAndTable schemaAndTable = new SchemaAndTable(tableMapEvent.getDatabase(), tableMapEvent.getTable());

        boolean shouldHandleTable = binlogEntryHandlers
                .stream()
                .map(BinlogEntryHandler::getSchemaAndTable)
                .anyMatch(schemaAndTable::equals);

        if (shouldHandleTable) {
          tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
        } else {
          tableMapEventByTableId.remove(tableMapEvent.getTableId());
        }

        break;
      }
      case EXT_WRITE_ROWS: {

        if (eventProcessingStartTime == null) {
          eventProcessingStartTime = System.nanoTime();
        }

        handleWriteRowsEvent(event);
        break;
      }
      case WRITE_ROWS: {

        if (eventProcessingStartTime == null) {
          eventProcessingStartTime = System.nanoTime();
        }

        handleWriteRowsEvent(event);
        break;
      }
      case EXT_UPDATE_ROWS: {
        break;
      }
      case UPDATE_ROWS: {
        break;
      }
      case ROTATE: {
        RotateEventData eventData = event.getData();
        if (eventData != null) {
          binlogFilename = eventData.getBinlogFilename();
        }
        break;
      }
    }
  }

  private void handleWriteRowsEvent(Event event) {
    if (rowsToSkip > 0) {
      rowsToSkip--;
      return;
    }


    WriteRowsEventData eventData = event.getData();

    if (tableMapEventByTableId.containsKey(eventData.getTableId())) {
      TableMapEventData tableMapEventData = tableMapEventByTableId.get(eventData.getTableId());

      SchemaAndTable schemaAndTable = new SchemaAndTable(tableMapEventData.getDatabase(), tableMapEventData.getTable());

      BinlogEntry entry = extractor.extract(schemaAndTable, eventData, binlogFilename, -1);

      binlogEntryHandlers
            .stream()
            .filter(bh -> bh.isFor(schemaAndTable))
            .forEach(binlogEntryHandler -> binlogEntryHandler.publish(entry));
    }
  }

  private void connectWithRetriesOnFail() {
    for (int i = 1;; i++) {
      try {
        client.connect(connectionTimeoutInMilliseconds);
        break;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private EventDeserializer getEventDeserializer() {
    EventDeserializer eventDeserializer = new EventDeserializer();

    Arrays.stream(EventType.values()).forEach(eventType -> {
      if (!SUPPORTED_EVENTS.contains(eventType)) {
        eventDeserializer.setEventDataDeserializer(eventType,
                new NullEventDataDeserializer());
      }
    });

    eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
            new WriteRowsDeserializer(tableMapEventByTableId, Mockito.mock(DbLogMetrics.class)));

    eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
            new WriteRowsDeserializer(tableMapEventByTableId, Mockito.mock(DbLogMetrics.class)).setMayContainExtraInformation(true));

    eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
            new UpdateRowsDeserializer(tableMapEventByTableId, Mockito.mock(DbLogMetrics.class)));

    eventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
            new UpdateRowsDeserializer(tableMapEventByTableId, Mockito.mock(DbLogMetrics.class)).setMayContainExtraInformation(true));

    return eventDeserializer;
  }
}
