package io.eventuate.local.mysql.binlog;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.local.common.*;
import io.eventuate.local.db.log.common.OffsetStore;
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
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import javax.sql.DataSource;
import java.util.Optional;

@Configuration
@EnableAutoConfiguration
@Import({EventuateKafkaPropertiesConfiguration.class,
        EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
        EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
        KafkaConsumerFactoryConfiguration.class,
        SqlDialectConfiguration.class})
public class MySqlBinlogCdcIntegrationTestConfiguration {

  @Bean
  public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
    return new EventuateSchema(eventuateDatabaseSchema);
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
  public SourceTableNameSupplier sourceTableNameSupplier(EventuateConfigurationProperties eventuateConfigurationProperties) {
    return new SourceTableNameSupplier(eventuateConfigurationProperties.getSourceTableName() == null ? "events" : eventuateConfigurationProperties.getSourceTableName());
  }

  @Bean
  public LeaderSelectorFactory connectorLeaderSelectorFactory(CuratorFramework curatorFramework) {
    return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
            new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback);
  }

  @Bean
  public MySqlBinaryLogClient mySqlBinaryLogClient(@Autowired MeterRegistry meterRegistry,
                                                   @Value("${spring.datasource.url}") String dataSourceURL,
                                                   DataSource dataSource,
                                                   EventuateConfigurationProperties eventuateConfigurationProperties,
                                                   OffsetStore offsetStore) {

    return new MySqlBinaryLogClient(
            meterRegistry,
            eventuateConfigurationProperties.getDbUserName(),
            eventuateConfigurationProperties.getDbPassword(),
            dataSourceURL,
            dataSource,
            eventuateConfigurationProperties.getReaderName(),
            eventuateConfigurationProperties.getMySqlBinlogClientUniqueId(),
            eventuateConfigurationProperties.getBinlogConnectionTimeoutInMilliseconds(),
            eventuateConfigurationProperties.getMaxAttemptsForBinlogConnection(),
            offsetStore,
            Optional.empty(),
            eventuateConfigurationProperties.getReplicationLagMeasuringIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryAttempts(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            eventuateConfigurationProperties.getOutboxId());
  }

  @Bean
  public BinlogEntryReaderLeadership binlogEntryReaderLeadership(EventuateConfigurationProperties eventuateConfigurationProperties,
                                                                 LeaderSelectorFactory leaderSelectorFactory,
                                                                 MySqlBinaryLogClient mySqlBinaryLogClient) {

    return new BinlogEntryReaderLeadership(eventuateConfigurationProperties.getLeadershipLockPath(),
            leaderSelectorFactory,
            mySqlBinaryLogClient);
  }

  @Bean
  public EventuateKafkaProducer eventuateKafkaProducer(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                       EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties) {
    return new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaProducerConfigurationProperties);
  }

  @Bean
  public DataProducerFactory dataProducerFactory(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                 EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties,
                                                 EventuateConfigurationProperties eventuateConfigurationProperties) {
    return () -> new EventuateKafkaDataProducerWrapper(new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaProducerConfigurationProperties),
            eventuateConfigurationProperties.isEnableBatchProcessing(),
            eventuateConfigurationProperties.getMaxBatchSize(),
            new LoggingMeterRegistry());
  }

  @Bean
  public PublishingStrategy<PublishedEvent> publishingStrategy() {
    return new PublishedEventPublishingStrategy();
  }

  @Bean
  public CdcDataPublisher<PublishedEvent> cdcKafkaPublisher(DataProducerFactory dataProducerFactory,
                                                            PublishingStrategy<PublishedEvent> publishingStrategy,
                                                            PublishingFilter publishingFilter) {

    return new CdcDataPublisher<>(dataProducerFactory,
            publishingFilter,
            publishingStrategy,
            new SimpleMeterRegistry());
  }

  @Bean
  public PublishingFilter publishingFilter(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                           EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                           KafkaConsumerFactory kafkaConsumerFactory) {
    return new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaConsumerConfigurationProperties,
            kafkaConsumerFactory);
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
  public TestHelper testHelper() {
    return new TestHelper();
  }
}
