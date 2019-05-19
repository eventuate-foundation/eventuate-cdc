package io.eventuate.local.db.log.test.common;

import io.eventuate.common.PublishedEvent;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.common.kafka.producer.EventuateKafkaProducer;
import io.eventuate.common.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.CdcKafkaPublisherEventsTest;


public abstract class AbstractDbLogBasedCdcKafkaPublisherEventsTest extends CdcKafkaPublisherEventsTest {

  @Override
  protected CdcDataPublisher<PublishedEvent> createCdcKafkaPublisher() {
    return new CdcDataPublisher<>(() ->
            new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(), EventuateKafkaProducerConfigurationProperties.empty()),
            new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(), EventuateKafkaConsumerConfigurationProperties.empty()),
            publishingStrategy,
            meterRegistry);
  }
}
