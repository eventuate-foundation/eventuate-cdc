package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.EventuateActiveMQDataProducerWrapper;
import io.eventuate.local.common.PublishingFilter;
import io.eventuate.messaging.activemq.spring.common.EventuateActiveMQCommonConfiguration;
import io.eventuate.messaging.activemq.spring.common.EventuateActiveMQConfigurationProperties;
import io.eventuate.messaging.activemq.producer.EventuateActiveMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import java.util.Optional;

@Configuration
@Profile("ActiveMQ")
@Import(EventuateActiveMQCommonConfiguration.class)
public class ActiveMQMessageTableChangesToDestinationsConfiguration {
  @Bean
  public PublishingFilter activeMQDuplicatePublishingDetector() {
    return (fileOffset, topic) -> true;
  }

  @Bean
  public DataProducerFactory activeMQDataProducerFactory(EventuateActiveMQConfigurationProperties eventuateActiveMQConfigurationProperties) {
    return () -> new EventuateActiveMQDataProducerWrapper(new EventuateActiveMQProducer(eventuateActiveMQConfigurationProperties.getUrl(),
            Optional.ofNullable(eventuateActiveMQConfigurationProperties.getUser()),
            Optional.ofNullable(eventuateActiveMQConfigurationProperties.getPassword())));
  }
}
