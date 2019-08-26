package io.eventuate.local.unified.cdc.pipeline.common.configuration;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.*;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CdcPipelineFactoryConfiguration {
  @Bean("eventuateLocalСdcPipelineFactory")
  public CdcPipelineFactory<PublishedEvent> defaultCdcPipelineFactory(BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider,
                                                                      CdcDataPublisher<PublishedEvent> cdcDataPublisher) {

    return new CdcPipelineFactory<>("eventuate-local",
            binlogEntryReaderLeadershipProvider,
            cdcDataPublisher,
            new BinlogEntryToPublishedEventConverter());
  }
}
