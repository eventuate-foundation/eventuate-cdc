package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinLogEvent;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class BinlogEntryHandler<EVENT extends BinLogEvent> {
  protected SchemaAndTable schemaAndTable;
  protected BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter;
  protected CdcDataPublisher<EVENT> cdcDataPublisher;

  public BinlogEntryHandler(SchemaAndTable schemaAndTable,
                            BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                            CdcDataPublisher<EVENT> cdcDataPublisher) {

    this.schemaAndTable = schemaAndTable;
    this.binlogEntryToEventConverter = binlogEntryToEventConverter;
    this.cdcDataPublisher = cdcDataPublisher;
  }

  public String getQualifiedTable() {
    return String.format("%s.%s", schemaAndTable.getSchema(), schemaAndTable.getTableName());
  }

  public SchemaAndTable getSchemaAndTable() {
    return schemaAndTable;
  }

  public boolean isFor(SchemaAndTable schemaAndTable) {
    return this.schemaAndTable.equals(schemaAndTable);
  }

  public Optional<CompletableFuture<?>> publish(BinlogEntry binlogEntry) {
    return cdcDataPublisher.handleEvent(binlogEntryToEventConverter.convert(binlogEntry));
  }
}
