package io.eventuate.local.postgres.wal;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.local.common.*;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelperConfiguration;
import io.eventuate.local.testutil.SqlScriptEditor;
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
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.util.stream.Collectors;

@Configuration
@EnableAutoConfiguration
@Import({EventuateKafkaPropertiesConfiguration.class,
        EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
        EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
        KafkaConsumerFactoryConfiguration.class,
        SqlDialectConfiguration.class,
        IdGeneratorConfiguration.class,
        TestHelperConfiguration.class})
public class PostgresWalCdcIntegrationTestConfiguration {

  @Bean
  public SourceTableNameSupplier sourceTableNameSupplier(EventuateConfigurationProperties eventuateConfigurationProperties) {
    return new SourceTableNameSupplier(eventuateConfigurationProperties.getSourceTableName() == null ? "events" : eventuateConfigurationProperties.getSourceTableName());
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public LeaderSelectorFactory connectorLeaderSelectorFactory(CuratorFramework curatorFramework) {
    return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
            new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback);
  }

  @Bean
  @DependsOnDatabaseInitialization
  public PostgresWalClient postgresWalClient(MeterRegistry meterRegistry,
                                             @Value("${spring.datasource.url}") String dbUrl,
                                             @Value("${spring.datasource.username}") String dbUserName,
                                             @Value("${spring.datasource.password}") String dbPassword,
                                             DataSource dataSource,
                                             EventuateConfigurationProperties eventuateConfigurationProperties) {

    return new PostgresWalClient(meterRegistry,
            dbUrl,
            dbUserName,
            dbPassword,
            eventuateConfigurationProperties.getPostgresWalIntervalInMilliseconds(),
            eventuateConfigurationProperties.getBinlogConnectionTimeoutInMilliseconds(),
            eventuateConfigurationProperties.getMaxAttemptsForBinlogConnection(),
            eventuateConfigurationProperties.getPostgresReplicationStatusIntervalInMilliseconds(),
            eventuateConfigurationProperties.getPostgresReplicationSlotName(),
            dataSource,
            eventuateConfigurationProperties.getReaderName(),
            eventuateConfigurationProperties.getReplicationLagMeasuringIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryAttempts(),
            eventuateConfigurationProperties.getAdditionalServiceReplicationSlotName(),
            eventuateConfigurationProperties.getWaitForOffsetSyncTimeoutInMilliseconds(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            eventuateConfigurationProperties.getOutboxId());
  }


  @Bean
  public CdcDataPublisher<PublishedEvent> dbLogBasedCdcKafkaPublisher(DataProducerFactory dataProducerFactory,
                                                                      EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                                      EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                                      PublishingStrategy<PublishedEvent> publishingStrategy,
                                                                      MeterRegistry meterRegistry,
                                                                      KafkaConsumerFactory kafkaConsumerFactory) {

    return new CdcDataPublisher<>(dataProducerFactory,
            new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
                    eventuateKafkaConsumerConfigurationProperties,
                    kafkaConsumerFactory),
            publishingStrategy,
            meterRegistry);
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
  public SqlScriptEditor sqlScriptEditor() {
    return sqlList -> {
      sqlList.set(0, sqlList.get(0).replace("CREATE SCHEMA", "CREATE SCHEMA IF NOT EXISTS"));

      return sqlList.stream().map(s -> s.replace("eventuate", "custom")).collect(Collectors.toList());
    };
  }

  @Bean
  public EventuateLocalZookeperConfigurationProperties eventuateLocalZookeperConfigurationProperties() {
    return new EventuateLocalZookeperConfigurationProperties();
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

}
