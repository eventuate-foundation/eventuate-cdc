package io.eventuate.tram.cdc.connector.pipeline;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.tram.cdc.connector.configuration.condition.EventuateTramCondition;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.tram.cdc.connector.MessageWithDestinationPublishingStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(EventuateTramCondition.class)
public class DefaultTramCdcPipelineFactoryConfiguration {
  @Bean("defaultCdcPipelineFactory")
  public CdcPipelineFactory defaultCdcPipelineFactory(DataProducerFactory dataProducerFactory,
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
