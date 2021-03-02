package io.eventuate.tram.cdc.connector.pipeline;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.tram.cdc.connector.configuration.condition.EventuateLocalCondition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Conditional(EventuateLocalCondition.class)
@Import(SqlDialectConfiguration.class)
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
