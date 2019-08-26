package io.eventuate.tram.cdc.connector.pipeline;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.tram.cdc.connector.MessageWithDestinationPublishingStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CdcTramPipelineFactoryConfiguration {
  @Bean("eventuateTramСdcPipelineFactory")
  public CdcPipelineFactory cdcPipelineFactory(DataProducerFactory dataProducerFactory,
                                               PublishingFilter publishingFilter,
                                               BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider,
                                               MeterRegistry meterRegistry) {

    return new CdcPipelineFactory<>("eventuate-tram",
            binlogEntryReaderLeadershipProvider,
            new CdcDataPublisher<>(dataProducerFactory,
                    publishingFilter,
                    new MessageWithDestinationPublishingStrategy(),
                    meterRegistry),
            new BinlogEntryToMessageConverter());
  }
}
