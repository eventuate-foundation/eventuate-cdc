package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.PublishingStrategy;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderLeadershipProvider;
import io.eventuate.local.unified.cdc.pipeline.common.DefaultSourceTableNameResolver;
import io.eventuate.local.unified.cdc.pipeline.common.health.BinlogEntryReaderHealthCheck;
import io.eventuate.local.unified.cdc.pipeline.common.health.CdcDataPublisherHealthCheck;
import io.eventuate.tram.cdc.connector.CdcProcessingStatusController;
import io.eventuate.tram.cdc.connector.MessageWithDestinationPublishingStrategy;
import io.eventuate.tram.cdc.connector.configuration.condition.EventuateLocalCondition;
import io.eventuate.tram.cdc.connector.configuration.condition.EventuateTramCondition;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonMessageTableChangesToDestinationsConfiguration {

  @Bean
  public CdcProcessingStatusController cdcProcessingStatusController(BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider) {
    return new CdcProcessingStatusController(binlogEntryReaderLeadershipProvider);
  }

  @Bean
  public BinlogEntryReaderHealthCheck binlogEntryReaderHealthCheck(BinlogEntryReaderLeadershipProvider binlogEntryReaderLeadershipProvider) {
    return new BinlogEntryReaderHealthCheck(binlogEntryReaderLeadershipProvider);
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
  public BinlogEntryReaderLeadershipProvider dbClientProvider() {
    return new BinlogEntryReaderLeadershipProvider();
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
