package io.eventuate.local.unified.cdc.pipeline.common.factory;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.BinlogEntryToEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.MessageCleaner;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.CdcPipeline;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;

public class CdcPipelineFactory<EVENT extends BinLogEvent> {

  private String type;
  private BinlogEntryReaderProvider binlogEntryReaderProvider;
  private CdcDataPublisher<EVENT> cdcDataPublisher;
  private BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter;
  private SqlDialectSelector sqlDialectSelector;

  public CdcPipelineFactory(String type,
                            BinlogEntryReaderProvider binlogEntryReaderProvider,
                            CdcDataPublisher<EVENT> cdcDataPublisher,
                            BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter,
                            SqlDialectSelector sqlDialectSelector) {
    this.type = type;
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
    this.cdcDataPublisher = cdcDataPublisher;
    this.binlogEntryToEventConverter = binlogEntryToEventConverter;
    this.sqlDialectSelector = sqlDialectSelector;
  }

  public boolean supports(String type) {
    return this.type.equals(type);
  }

  public CdcPipeline<EVENT> create(CdcPipelineProperties cdcPipelineProperties) {
    BinlogEntryReader binlogEntryReader = binlogEntryReaderProvider
            .get(cdcPipelineProperties.getReader())
            .getBinlogEntryReader();

    EventuateSchema eventuateSchema = new EventuateSchema(cdcPipelineProperties.getEventuateDatabaseSchema());

    binlogEntryReader.addBinlogEntryHandler(eventuateSchema,
            cdcPipelineProperties.getSourceTableName(),
            binlogEntryToEventConverter,
            cdcDataPublisher);

    EventuateSqlDialect eventuateSqlDialect = sqlDialectSelector
            .getDialect(binlogEntryReaderProvider
                    .getReaderProperties(cdcPipelineProperties.getReader()).getDataSourceDriverClassName());

    return new CdcPipeline<>(cdcDataPublisher,
            new MessageCleaner(eventuateSqlDialect,
                    binlogEntryReader.getDataSource(),
                    eventuateSchema,
                    cdcPipelineProperties.getPurgeMessagesEnabled(),
                    cdcPipelineProperties.getPurgeMessagesMaxAgeInSeconds(),
                    cdcPipelineProperties.getPurgeReceivedMessagesEnabled(),
                    cdcPipelineProperties.getPurgeReceivedMessagesMaxAgeInSeconds(),
                    cdcPipelineProperties.getPurgeIntervalInSeconds()));
  }
}
