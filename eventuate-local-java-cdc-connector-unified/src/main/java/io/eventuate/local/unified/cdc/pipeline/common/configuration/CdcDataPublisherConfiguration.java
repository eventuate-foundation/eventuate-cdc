package io.eventuate.local.unified.cdc.pipeline.common.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CdcDataPublisherConfiguration {

  @Bean
  public CdcDataPublisher<PublishedEvent> cdcDataPublisher(DataProducerFactory dataProducerFactory,
                                                           PublishingFilter publishingFilter,
                                                           MeterRegistry meterRegistry) {

    return new CdcDataPublisher<>(dataProducerFactory,
                    publishingFilter,
                    new PublishedEventPublishingStrategy(),
                    meterRegistry);
  }
}
