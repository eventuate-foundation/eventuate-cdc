package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.jdbc.SchemaAndTable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class BinlogEntryHandler<EVENT extends BinLogEvent> {
  protected SchemaAndTable schemaAndTable;
  protected BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter;
  protected Function<EVENT, CompletableFuture<?>> eventPublisher;

  public BinlogEntryHandler(SchemaAndTable schemaAndTable,
                            BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                            Function<EVENT, CompletableFuture<?>> eventPublisher) {

    this.schemaAndTable = schemaAndTable;
    this.binlogEntryToEventConverter = binlogEntryToEventConverter;
    this.eventPublisher = eventPublisher;
  }

  public String getQualifiedTable() {
    return "%s.%s".formatted(schemaAndTable.getSchema(), schemaAndTable.getTableName());
  }

  public SchemaAndTable getSchemaAndTable() {
    return schemaAndTable;
  }

  public boolean isFor(SchemaAndTable schemaAndTable) {
    return this.schemaAndTable.equals(schemaAndTable);
  }

  public CompletableFuture<?> publish(BinlogEntry binlogEntry, Integer partitionOffset) {
    return binlogEntryToEventConverter
            .convert(binlogEntry, partitionOffset)
            .map(eventPublisher::apply)
            .orElse(CompletableFuture.completedFuture(null));
  }

  public String getDestinationColumn() {
    return binlogEntryToEventConverter.getDestinationColumn();
  }
}
