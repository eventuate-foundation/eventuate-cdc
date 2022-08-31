package io.eventuate.local.unified.cdc.pipeline.polling.configuration;

import io.eventuate.common.jdbc.OutboxPartitioningSpec;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.common.configuration.CommonCdcDefaultPipelineReaderConfiguration;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.eventuate.local.unified.cdc.pipeline.polling.factory.PollingCdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.polling.properties.PollingPipelineReaderProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Arrays;
import java.util.HashSet;

@Configuration
public class PollingCdcPipelineReaderConfiguration extends CommonCdcDefaultPipelineReaderConfiguration {

  @Bean("eventuateLocalPollingCdcPipelineReaderFactory")
  public CdcPipelineReaderFactory pollingCdcPipelineReaderFactory(MeterRegistry meterRegistry,
                                                                  SqlDialectSelector sqlDialectSelector,
                                                                  ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties) {

    return new PollingCdcPipelineReaderFactory(meterRegistry,
            sqlDialectSelector,
            connectionPoolConfigurationProperties);
  }

  @Profile("EventuatePolling")
  @Bean("defaultCdcPipelineReaderFactory")
  public CdcPipelineReaderFactory defaultPollingCdcPipelineReaderFactory(MeterRegistry meterRegistry,
                                                                         SqlDialectSelector sqlDialectSelector,
                                                                         ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties) {

    return new PollingCdcPipelineReaderFactory(meterRegistry,
            sqlDialectSelector,
            connectionPoolConfigurationProperties);
  }

  @Profile("EventuatePolling")
  @Bean
  public CdcPipelineReaderProperties defaultPollingPipelineReaderProperties() {
    PollingPipelineReaderProperties pollingPipelineReaderProperties = createPollingPipelineReaderProperties();

    pollingPipelineReaderProperties.setType(PollingCdcPipelineReaderFactory.TYPE);

    initCdcPipelineReaderProperties(pollingPipelineReaderProperties);

    return pollingPipelineReaderProperties;
  }

  private PollingPipelineReaderProperties createPollingPipelineReaderProperties() {
    PollingPipelineReaderProperties pollingPipelineReaderProperties = new PollingPipelineReaderProperties();

    pollingPipelineReaderProperties.setPollingIntervalInMilliseconds(eventuateConfigurationProperties.getPollingIntervalInMilliseconds());
    pollingPipelineReaderProperties.setMaxEventsPerPolling(eventuateConfigurationProperties.getMaxEventsPerPolling());
    pollingPipelineReaderProperties.setMaxAttemptsForPolling(eventuateConfigurationProperties.getMaxAttemptsForPolling());
    pollingPipelineReaderProperties.setPollingRetryIntervalInMilliseconds(eventuateConfigurationProperties.getPollingRetryIntervalInMilliseconds());
    pollingPipelineReaderProperties.setPollingParallelChannels(new HashSet<>(Arrays.asList(eventuateConfigurationProperties.getPollingParallelChannels())));

    pollingPipelineReaderProperties.setOutboxPartitioning(new OutboxPartitioningSpec(eventuateConfigurationProperties.getOutboxTables(), eventuateConfigurationProperties.getOutboxTablePartitions()));

    return pollingPipelineReaderProperties;
  }
}
