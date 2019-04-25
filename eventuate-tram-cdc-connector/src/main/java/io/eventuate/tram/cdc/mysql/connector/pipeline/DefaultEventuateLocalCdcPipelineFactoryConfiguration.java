package io.eventuate.tram.cdc.mysql.connector.pipeline;

import io.eventuate.common.PublishedEvent;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.tram.cdc.mysql.connector.configuration.condition.EventuateLocalCondition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(EventuateLocalCondition.class)
public class DefaultEventuateLocalCdcPipelineFactoryConfiguration {
  @Bean("defaultCdcPipelineFactory")
  public CdcPipelineFactory<PublishedEvent> defaultCdcPipelineFactory(BinlogEntryReaderProvider binlogEntryReaderProvider,
                                                                      CdcDataPublisher<PublishedEvent> cdcDataPublisher) {

    return new CdcPipelineFactory<>("eventuate-local",
            binlogEntryReaderProvider,
            cdcDataPublisher,
            new BinlogEntryToPublishedEventConverter());
  }
}
