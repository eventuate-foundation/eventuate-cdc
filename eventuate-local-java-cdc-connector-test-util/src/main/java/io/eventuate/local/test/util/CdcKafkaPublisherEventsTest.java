package io.eventuate.local.test.util;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.PublishingStrategy;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.Collections;


public abstract class CdcKafkaPublisherEventsTest {

  @Autowired
  protected MeterRegistry meterRegistry;

  @Autowired
  protected EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  protected KafkaConsumerFactory kafkaConsumerFactory;

  @Autowired
  protected EventuateConfigurationProperties eventuateConfigurationProperties;

  @Autowired
  protected PublishingStrategy<PublishedEvent> publishingStrategy;

  protected CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  @Autowired
  protected EventuateSchema eventuateSchema;

  @Autowired
  protected SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected IdGenerator idGenerator;

  @Before
  public void init() {
    cdcDataPublisher = createCdcKafkaPublisher();
    cdcDataPublisher.start();
  }

  @Test
  public void shouldSendPublishedEventsToKafka() {
    TestHelper.EventIdEntityId entityIdVersionAndEventIds = testHelper.saveEvent(testHelper.generateTestCreatedEvent());

    KafkaConsumer<String, byte[]> consumer = testHelper.createConsumer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    consumer.partitionsFor(testHelper.getEventTopicName());
    consumer.subscribe(Collections.singletonList(testHelper.getEventTopicName()));

    testHelper.waitForEventInKafka(consumer, entityIdVersionAndEventIds.getEntityId(), LocalDateTime.now().plusSeconds(40));
    cdcDataPublisher.stop();
  }

  public abstract void clear();

  protected abstract CdcDataPublisher<PublishedEvent> createCdcKafkaPublisher();
}
