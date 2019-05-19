package io.eventuate.local.mysql.binlog;

import io.eventuate.common.kafka.EventuateKafkaConfigurationProperties;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.common.kafka.producer.EventuateKafkaProducer;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.db.log.common.DatabaseOffsetKafkaStore;
import io.eventuate.local.db.log.common.OffsetStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class KafkaOffsetStoreConfiguration {

  @Bean
  @Primary
  public OffsetStore offsetStore(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                 EventuateConfigurationProperties eventuateConfigurationProperties,
                                 EventuateKafkaProducer eventuateKafkaProducer,
                                 EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {

    return new DatabaseOffsetKafkaStore(eventuateConfigurationProperties.getOffsetStorageTopicName(),
            eventuateConfigurationProperties.getOffsetStoreKey(),
            eventuateKafkaProducer,
            eventuateKafkaConfigurationProperties,
            eventuateKafkaConsumerConfigurationProperties);
  }
}
