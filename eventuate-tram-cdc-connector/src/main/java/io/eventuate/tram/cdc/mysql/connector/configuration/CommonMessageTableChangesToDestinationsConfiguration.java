package io.eventuate.tram.cdc.mysql.connector.configuration;

import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.PublishingStrategy;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.DefaultSourceTableNameResolver;
import io.eventuate.local.unified.cdc.pipeline.common.health.BinlogEntryReaderHealthCheck;
import io.eventuate.local.unified.cdc.pipeline.common.health.CdcDataPublisherHealthCheck;
import io.eventuate.tram.cdc.mysql.connector.CdcProcessingStatusController;
import io.eventuate.tram.cdc.mysql.connector.MessageWithDestination;
import io.eventuate.tram.cdc.mysql.connector.MessageWithDestinationPublishingStrategy;
import io.eventuate.tram.cdc.mysql.connector.configuration.condition.EventuateLocalCondition;
import io.eventuate.tram.cdc.mysql.connector.configuration.condition.EventuateTramCondition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonMessageTableChangesToDestinationsConfiguration {

  @Bean
  public CdcProcessingStatusController cdcProcessingStatusController(BinlogEntryReaderProvider binlogEntryReaderProvider) {
    return new CdcProcessingStatusController(binlogEntryReaderProvider);
  }

  @Bean
  public BinlogEntryReaderHealthCheck binlogEntryReaderHealthCheck(BinlogEntryReaderProvider binlogEntryReaderProvider) {
    return new BinlogEntryReaderHealthCheck(binlogEntryReaderProvider);
  }

  @Bean
  public CdcDataPublisherHealthCheck cdcDataPublisherHealthCheck(CdcDataPublisher cdcDataPublisher) {
    return new CdcDataPublisherHealthCheck(cdcDataPublisher);
  }

  @Bean
  @Conditional(EventuateTramCondition.class)
  public DefaultSourceTableNameResolver eventuateTramDefaultSourceTableNameResolver() {
    return pipelineType -> {
      if ("eventuate-tram".equals(pipelineType) || "default".equals(pipelineType)) return "message";
      if ("eventuate-local".equals(pipelineType)) return "events";

      throw new RuntimeException(String.format("Unknown pipeline type '%s'", pipelineType));
    };
  }

  @Bean
  @Conditional(EventuateLocalCondition.class)
  public DefaultSourceTableNameResolver eventuateLocalDefaultSourceTableNameResolver() {
    return pipelineType -> {
      if ("eventuate-tram".equals(pipelineType)) return "message";
      if ("eventuate-local".equals(pipelineType) || "default".equals(pipelineType)) return "events";

      throw new RuntimeException(String.format("Unknown pipeline type '%s'", pipelineType));
    };
  }

  @Bean
  public BinlogEntryReaderProvider dbClientProvider() {
    return new BinlogEntryReaderProvider();
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public PublishingStrategy<MessageWithDestination> publishingStrategy() {
    return new MessageWithDestinationPublishingStrategy();
  }
}
