package io.eventuate.local.db.log.test.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = DuplicatePublishingDetectorTest.Config.class)
@EnableAutoConfiguration
public class DuplicatePublishingDetectorTest {

  @Configuration
  public static class Config {
    @Bean
    public EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties() {
      return new EventuateKafkaConfigurationProperties();
    }

    @Bean
    public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
      return new EventuateSchema(eventuateDatabaseSchema);
    }

    @Bean
    public TestHelper testHelper() {
      return new TestHelper();
    }
  }

  @Autowired
  EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  TestHelper testHelper;

  @Test
  public void emptyTopicTest() {
    DuplicatePublishingDetector duplicatePublishingDetector = new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    BinlogFileOffset bfo = testHelper.generateBinlogFileOffset();

    assertTrue(duplicatePublishingDetector.shouldBePublished(bfo, testHelper.generateUniqueTopicName()));
  }

  @Test
  public void shouldBePublishedTest() {
    String topicName = testHelper.generateUniqueTopicName();
    String binlogFilename = "binlog.file." + System.currentTimeMillis();
    DuplicatePublishingDetector duplicatePublishingDetector = new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    Producer<String, String> producer = testHelper.createProducer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    floodTopic(producer, binlogFilename, topicName);
    producer.close();

    assertFalse(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 1L), topicName));
    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 10L), topicName));
  }

  @Test
  public void shouldHandlePublishCheckForOldEntires() {
    String topicName = testHelper.generateUniqueTopicName();
    String binlogFilename = "binlog.file." + System.currentTimeMillis();
    DuplicatePublishingDetector duplicatePublishingDetector = new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    Producer<String, String> producer = testHelper.createProducer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    floodTopic(producer, binlogFilename, topicName);
    sendOldPublishedEvent(producer, topicName);
    producer.close();

    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 10L), topicName));
  }

  private void floodTopic(Producer<String, String> producer, String binlogFilename, String topicName) {
    for (int i = 0; i < 10; i++) {
      PublishedEvent publishedEvent = new PublishedEvent();
      publishedEvent.setEntityId(UUID.randomUUID().toString());
      publishedEvent.setBinlogFileOffset(new BinlogFileOffset(binlogFilename, (long)i));
      String json = JSonMapper.toJson(publishedEvent);
      producer.send(
              new ProducerRecord<>(topicName,
                      publishedEvent.getEntityId(),
                      json));

    }

  }

  private void sendOldPublishedEvent(Producer<String, String> producer, String topicName) {
    for (int i = 0; i < 10; i++) {
      PublishedEvent publishedEvent = new PublishedEvent();
      publishedEvent.setEntityId(UUID.randomUUID().toString());
      String json = JSonMapper.toJson(publishedEvent);
      producer.send(
              new ProducerRecord<>(topicName,
                      publishedEvent.getEntityId(),
                      json));
    }
  }

}
