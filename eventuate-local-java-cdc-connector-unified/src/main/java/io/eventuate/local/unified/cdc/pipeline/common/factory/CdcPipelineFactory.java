package io.eventuate.local.unified.cdc.pipeline.common.factory;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.CdcPipeline;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;

public class CdcPipelineFactory<EVENT extends BinLogEvent> {

  private String type;
  private BinlogEntryReaderProvider binlogEntryReaderProvider;
  private CdcDataPublisher<EVENT> cdcDataPublisher;
  private BinlogEntryToEventConverterFactory<EVENT> binlogEntryToEventConverterFactory;

  public CdcPipelineFactory(String type,
                            BinlogEntryReaderProvider binlogEntryReaderProvider,
                            CdcDataPublisher<EVENT> cdcDataPublisher,
                            BinlogEntryToEventConverterFactory<EVENT> binlogEntryToEventConverterFactory) {
    this.type = type;
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
    this.cdcDataPublisher = cdcDataPublisher;
    this.binlogEntryToEventConverterFactory = binlogEntryToEventConverterFactory;
  }

  public boolean supports(String type) {
    return this.type.equals(type);
  }

  public CdcPipeline<EVENT> create(CdcPipelineProperties cdcPipelineProperties) {
    BinlogEntryReader binlogEntryReader = binlogEntryReaderProvider
            .getRequired(cdcPipelineProperties.getReader())
            .getBinlogEntryReader();

    EventuateSchema eventuateSchema = new EventuateSchema(cdcPipelineProperties.getEventuateDatabaseSchema());

    binlogEntryReader.addBinlogEntryHandler(eventuateSchema,
            cdcPipelineProperties.getSourceTableName(),
            binlogEntryToEventConverterFactory.apply(binlogEntryReader.getOutboxId()),
            cdcDataPublisher::sendMessage);

    return new CdcPipeline<>(cdcDataPublisher);
  }
}
