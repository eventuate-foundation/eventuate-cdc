package io.eventuate.tram.cdc.connector.pipeline;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.id.DatabaseIdGenerator;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.tram.cdc.connector.MessageWithDestinationPublishingStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(SqlDialectConfiguration.class)
public class CdcTramPipelineFactoryConfiguration {
  @Bean("eventuateTramСdcPipelineFactory")
  public CdcPipelineFactory cdcPipelineFactory(DataProducerFactory dataProducerFactory,
                                               PublishingFilter publishingFilter,
                                               BinlogEntryReaderProvider binlogEntryReaderProvider,
                                               MeterRegistry meterRegistry) {

    return new CdcPipelineFactory<>("eventuate-tram",
            binlogEntryReaderProvider,
            new CdcDataPublisher<>(dataProducerFactory,
                    publishingFilter,
                    new MessageWithDestinationPublishingStrategy(),
                    meterRegistry),
            outboxId -> new BinlogEntryToMessageConverter(new DatabaseIdGenerator(outboxId)));
  }
}
