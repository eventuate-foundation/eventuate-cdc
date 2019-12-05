package io.eventuate.local.db.log.test.common;

import io.eventuate.cdc.producer.wrappers.DataProducerFactory;
import io.eventuate.cdc.producer.wrappers.kafka.EventuateKafkaDataProducerWrapper;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.CdcKafkaPublisherEventsTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;

public abstract class AbstractDbLogBasedCdcKafkaPublisherEventsTest extends CdcKafkaPublisherEventsTest {

  @Override
  protected CdcDataPublisher<PublishedEvent> createCdcKafkaPublisher() {
    DataProducerFactory dataProducerFactory = () -> new EventuateKafkaDataProducerWrapper(createEventuateKafkaProducer(),
            eventuateConfigurationProperties.isEnableBatchProcessing(),
            eventuateConfigurationProperties.getMaxBatchSize());

    DuplicatePublishingDetector duplicatePublishingDetector =
            new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
                    EventuateKafkaConsumerConfigurationProperties.empty());

    return new CdcDataPublisher<>(dataProducerFactory,
            duplicatePublishingDetector,
            publishingStrategy,
            meterRegistry);
  }

  private EventuateKafkaProducer createEventuateKafkaProducer() {
    return new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaProducerConfigurationProperties.empty());
  }
}
