package io.eventuate.local.unified.cdc.pipeline.common.factory;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.BinlogEntryToEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import io.eventuate.local.unified.cdc.pipeline.common.CdcPipeline;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;

public class CdcPipelineFactory<EVENT extends BinLogEvent> {

  private String type;
  private BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider;
  private CdcDataPublisher<EVENT> cdcDataPublisher;
  private BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter;

  public CdcPipelineFactory(String type,
                            BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider,
                            CdcDataPublisher<EVENT> cdcDataPublisher,
                            BinlogEntryToEventConverter<EVENT> binlogEntryToEventConverter) {
    this.type = type;
    this.binlogEntryReaderLeadershipProvider = binlogEntryReaderLeadershipProvider;
    this.cdcDataPublisher = cdcDataPublisher;
    this.binlogEntryToEventConverter = binlogEntryToEventConverter;
  }

  public boolean supports(String type) {
    return this.type.equals(type);
  }

  public CdcPipeline<EVENT> create(CdcPipelineProperties cdcPipelineProperties) {
    BinlogEntryReader binlogEntryReader = binlogEntryReaderLeadershipProvider
            .getBinlogEntryReaderLeadership(cdcPipelineProperties.getReader())
            .getBinlogEntryReader();

    binlogEntryReader.addBinlogEntryHandler(new EventuateSchema(cdcPipelineProperties.getEventuateDatabaseSchema()),
            cdcPipelineProperties.getSourceTableName(),
            binlogEntryToEventConverter,
            cdcDataPublisher);

    return new CdcPipeline<>(cdcDataPublisher);
  }
}
