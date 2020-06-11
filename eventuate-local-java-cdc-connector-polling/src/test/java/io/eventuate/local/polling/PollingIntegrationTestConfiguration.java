package io.eventuate.local.polling;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.local.common.*;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Configuration
@EnableAutoConfiguration
@Import({EventuateKafkaPropertiesConfiguration.class,
        EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
        EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
        SqlDialectConfiguration.class,
        KafkaConsumerFactoryConfiguration.class})
public class PollingIntegrationTestConfiguration {

  @Bean
  public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
    return new EventuateSchema(eventuateDatabaseSchema);
  }

  @Bean
  public SourceTableNameSupplier sourceTableNameSupplier(EventuateConfigurationProperties eventuateConfigurationProperties) {
    return new SourceTableNameSupplier(eventuateConfigurationProperties.getSourceTableName() == null ? "events" : eventuateConfigurationProperties.getSourceTableName());
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public EventuateLocalZookeperConfigurationProperties eventuateLocalZookeperConfigurationProperties() {
    return new EventuateLocalZookeperConfigurationProperties();
  }

  @Bean
  public EventuateKafkaProducer eventuateKafkaProducer(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                       EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties) {
    return new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaProducerConfigurationProperties);
  }

  @Bean
  public PublishingStrategy<PublishedEvent> publishingStrategy() {
    return new PublishedEventPublishingStrategy();
  }

  @Bean
  @Profile("EventuatePolling")
  public PollingDataProvider pollingDataProvider() {
    return new EventPollingDataProvider();
  }

  @Bean
  @Profile("EventuatePolling")
  public PollingDao pollingDao(@Autowired(required = false) MeterRegistry meterRegistry,
                               @Value("${spring.datasource.url}") String dataSourceURL,
                               @Value("${spring.datasource.driver-class-name}") String driver,
                               EventuateConfigurationProperties eventuateConfigurationProperties,
                               DataSource dataSource,
                               SqlDialectSelector sqlDialectSelector) {

    return new PollingDao(meterRegistry,
            dataSourceURL,
            dataSource,
            eventuateConfigurationProperties.getMaxEventsPerPolling(),
            eventuateConfigurationProperties.getMaxAttemptsForPolling(),
            eventuateConfigurationProperties.getPollingRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getPollingIntervalInMilliseconds(),
            eventuateConfigurationProperties.getReaderName(),
            sqlDialectSelector.getDialect(driver));
  }

  @Bean
  public CuratorFramework curatorFramework(EventuateLocalZookeperConfigurationProperties eventuateLocalZookeperConfigurationProperties) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.
            builder().retryPolicy(retryPolicy)
            .connectString(eventuateLocalZookeperConfigurationProperties.getConnectionString())
            .build();
    client.start();
    return client;
  }

  @Bean
  public LeaderSelectorFactory connectorLeaderSelectorFactory(CuratorFramework curatorFramework) {
    return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
            new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback);
  }

  @Bean
  public DuplicatePublishingDetector duplicatePublishingDetector(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                                 EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                                 KafkaConsumerFactory kafkaConsumerFactory) {

    return new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaConsumerConfigurationProperties,
            kafkaConsumerFactory);
  }

  @Bean
  public TestHelper testHelper() {
    return new TestHelper();
  }
}
