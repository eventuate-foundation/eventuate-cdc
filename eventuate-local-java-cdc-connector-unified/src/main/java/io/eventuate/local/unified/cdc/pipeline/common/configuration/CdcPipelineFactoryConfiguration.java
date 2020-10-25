package io.eventuate.local.unified.cdc.pipeline.common.configuration;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.DatabaseIdGenerator;
import io.eventuate.local.common.*;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CdcPipelineFactoryConfiguration {
  @Bean("eventuateLocalСdcPipelineFactory")
  public CdcPipelineFactory<PublishedEvent> defaultCdcPipelineFactory(BinlogEntryReaderProvider binlogEntryReaderProvider,
                                                                      CdcDataPublisher<PublishedEvent> cdcDataPublisher) {

    return new CdcPipelineFactory<>("eventuate-local",
            binlogEntryReaderProvider,
            cdcDataPublisher,
            outboxId -> new BinlogEntryToPublishedEventConverter(new DatabaseIdGenerator(outboxId)));
  }
}
