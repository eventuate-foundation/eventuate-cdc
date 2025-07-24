package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.local.db.log.common.DatabaseOffsetKafkaStore;
import io.eventuate.local.mysql.binlog.DebeziumBinlogOffsetKafkaStore;
import io.eventuate.local.unified.cdc.pipeline.common.health.KafkaHealthCheck;
import io.eventuate.local.unified.cdc.pipeline.dblog.common.factory.OffsetStoreFactory;
import io.eventuate.local.unified.cdc.pipeline.dblog.mysqlbinlog.factory.DebeziumOffsetStoreFactory;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import io.eventuate.tram.cdc.connector.configuration.condition.KafkaCondition;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EventuateKafkaPropertiesConfiguration.class,
        EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
        EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
        KafkaConsumerFactoryConfiguration.class})
@Conditional(KafkaCondition.class)
public class KafkaMessageTableChangesToDestinationsConfiguration {
  @Bean
  public PublishingFilter kafkaDuplicatePublishingDetector(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                           EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                           KafkaConsumerFactory kafkaConsumerFactory) {
    return new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaConsumerConfigurationProperties,
            kafkaConsumerFactory);
  }

  @Bean
  public DataProducerFactory kafkaDataProducerFactory(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                      EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties,
                                                      EventuateConfigurationProperties eventuateConfigurationProperties,
                                                      MeterRegistry meterRegistry) {
    return () -> new EventuateKafkaDataProducerWrapper(new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            eventuateKafkaProducerConfigurationProperties),
            eventuateConfigurationProperties.isEnableBatchProcessing(),
            eventuateConfigurationProperties.getMaxBatchSize(),
            meterRegistry);
  }

  @Bean
  public EventuateKafkaProducer eventuateKafkaProducer(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                       EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties) {
    return new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(), eventuateKafkaProducerConfigurationProperties);
  }

  @Bean
  public KafkaHealthCheck kafkaHealthCheck() {
    return new KafkaHealthCheck();
  }

  @Bean
  public DebeziumOffsetStoreFactory debeziumOffsetStoreFactory(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                               EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {

    return () ->
            new DebeziumBinlogOffsetKafkaStore(eventuateKafkaConfigurationProperties,
                    eventuateKafkaConsumerConfigurationProperties);
  }

  @Bean
  @DependsOnDatabaseInitialization
  public OffsetStoreFactory postgresWalKafkaOffsetStoreFactory(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                               EventuateKafkaProducer eventuateKafkaProducer,
                                                               EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {

    return (properties, dataSource, eventuateSchema, clientName) ->  new DatabaseOffsetKafkaStore(properties.getOffsetStorageTopicName(),
            clientName,
            eventuateKafkaProducer,
            eventuateKafkaConfigurationProperties,
            eventuateKafkaConsumerConfigurationProperties);
  }
}
