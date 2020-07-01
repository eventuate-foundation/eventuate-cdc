package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.EventuateRabbitMQDataProducerWrapper;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducer;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducerConfigurationProperties;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducerConfigurationPropertiesConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("RabbitMQ")
@Import(EventuateRabbitMQProducerConfigurationPropertiesConfiguration.class)
public class RabbitMQMessageTableChangesToDestinationsConfiguration {
  @Bean
  public PublishingFilter rabbitMQDuplicatePublishingDetector() {
    return (fileOffset, topic) -> true;
  }

  @Bean
  public DataProducerFactory rabbitMQDataProducerFactory(EventuateRabbitMQProducerConfigurationProperties properties) {
    return () -> new EventuateRabbitMQDataProducerWrapper(new EventuateRabbitMQProducer(properties.getBrokerAddresses()));
  }
}
