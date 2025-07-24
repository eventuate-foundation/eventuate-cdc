package io.eventuate.tram.cdc.connector.pipeline;

import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.CdcMessageCleanerConfigurator;
import io.eventuate.local.unified.cdc.pipeline.PipelineConfigPropertiesProvider;
import io.eventuate.local.unified.cdc.pipeline.UnifiedCdcConfigurator;
import io.eventuate.local.unified.cdc.pipeline.common.configuration.CdcDataPublisherConfiguration;
import io.eventuate.local.unified.cdc.pipeline.common.configuration.CdcDefaultPipelinePropertiesConfiguration;
import io.eventuate.local.unified.cdc.pipeline.common.configuration.CdcPipelineFactoryConfiguration;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import io.eventuate.local.unified.cdc.pipeline.dblog.mysqlbinlog.configuration.MySqlBinlogCdcPipelineReaderConfiguration;
import io.eventuate.local.unified.cdc.pipeline.dblog.postgreswal.configuration.PostgresWalCdcPipelineReaderConfiguration;
import io.eventuate.local.unified.cdc.pipeline.polling.configuration.PollingCdcPipelineReaderConfiguration;
import io.eventuate.tram.cdc.connector.configuration.MessageTableChangesToDestinationsConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.util.Collection;

@AutoConfiguration
@Import({MessageTableChangesToDestinationsConfiguration.class,

        CdcDataPublisherConfiguration.class,

        CdcDefaultPipelinePropertiesConfiguration.class,
        CdcPipelineFactoryConfiguration.class,
        DefaultEventuateLocalCdcPipelineFactoryConfiguration.class,
        DefaultTramCdcPipelineFactoryConfiguration.class,
        CdcTramPipelineFactoryConfiguration.class,

        MySqlBinlogCdcPipelineReaderConfiguration.class,
        PollingCdcPipelineReaderConfiguration.class,
        PostgresWalCdcPipelineReaderConfiguration.class})
@EnableConfigurationProperties({RawUnifiedCdcProperties.class, ConnectionPoolConfigurationProperties.class})
public class CdcTramPipelineConfiguration {
  @Bean
  public UnifiedCdcConfigurator unifiedCdcConfigurator() {
    return new UnifiedCdcConfigurator();
  }

  @Bean
  public PipelineConfigPropertiesProvider pipelineConfigPropertiesProvider(RawUnifiedCdcProperties rawUnifiedCdcProperties, Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories) {
    return new PipelineConfigPropertiesProvider(rawUnifiedCdcProperties, cdcPipelineReaderFactories);
  }

  @Bean
  public CdcMessageCleanerConfigurator cdcMessageCleanerConfigurator() {
    return new CdcMessageCleanerConfigurator();
  }
}
