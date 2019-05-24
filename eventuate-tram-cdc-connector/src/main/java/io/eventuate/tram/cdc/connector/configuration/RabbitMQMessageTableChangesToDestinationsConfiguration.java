package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.EventuateRabbitMQDataProducerWrapper;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.messaging.rabbitmq.producer.EventuateRabbitMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("RabbitMQ")
public class RabbitMQMessageTableChangesToDestinationsConfiguration {
  @Bean
  public PublishingFilter rabbitMQDuplicatePublishingDetector() {
    return (fileOffset, topic) -> true;
  }

  @Bean
  public DataProducerFactory rabbitMQDataProducerFactory(@Value("${rabbitmq.url}") String rabbitMQURL) {
    return () -> new EventuateRabbitMQDataProducerWrapper(new EventuateRabbitMQProducer(rabbitMQURL));
  }
}
