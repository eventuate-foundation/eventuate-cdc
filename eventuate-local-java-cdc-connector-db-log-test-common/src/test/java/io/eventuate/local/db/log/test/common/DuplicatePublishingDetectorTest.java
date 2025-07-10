package io.eventuate.local.db.log.test.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.DuplicatePublishingDetector;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.TestHelperConfiguration;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = DuplicatePublishingDetectorTest.Config.class)
@EnableAutoConfiguration
public class DuplicatePublishingDetectorTest {

  private String topicName;
  private String binlogFilename;
  private DuplicatePublishingDetector duplicatePublishingDetector;

  @Configuration
  @Import({EventuateKafkaPropertiesConfiguration.class,
          KafkaConsumerFactoryConfiguration.class,
          SqlDialectConfiguration.class,
          IdGeneratorConfiguration.class,
          TestHelperConfiguration.class})
  public static class Config {

  }

  @Autowired
  EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  KafkaConsumerFactory kafkaConsumerFactory;

  @Autowired
  TestHelper testHelper;

  @BeforeEach
  public void setUp() {
    topicName = testHelper.generateUniqueTopicName();
    binlogFilename = "binlog.file." + System.currentTimeMillis();
    duplicatePublishingDetector = makeDuplicatePublishingDetector();
  }

  @Test
  public void emptyTopicTest() {

    BinlogFileOffset bfo = testHelper.generateBinlogFileOffset();

    assertTrue(duplicatePublishingDetector.shouldBePublished(bfo, topicName));
  }

  private DuplicatePublishingDetector makeDuplicatePublishingDetector() {
    return new DuplicatePublishingDetector(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty(), kafkaConsumerFactory);
  }

  @Test
  public void shouldBePublishedTest() {
    Producer<String, String> producer = testHelper.createProducer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    floodTopic(producer, binlogFilename, topicName);
    producer.close();

    assertFalse(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 1L), topicName));
    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 10L), topicName));
  }

  /*

  These tests are for manually testing with a topic that is empty because the retention time has passed
  It seems too unpredictable to automate.

  @Test
  public void shouldHandleTopicWithExpiredMessages() throws ExecutionException, InterruptedException {
    String subscriberId = "duplicate-checker-" + topicName + "-" + System.currentTimeMillis();
    Properties consumerProperties = ConsumerPropertiesFactory.makeDefaultConsumerProperties(eventuateKafkaConfigurationProperties.getBootstrapServers(), subscriberId);
    consumerProperties.putAll(EventuateKafkaConsumerConfigurationProperties.empty().getProperties());

    AdminClient adminClient = AdminClient.create(consumerProperties);
    NewTopic topic = new NewTopic(topicName, 3, (short)1).configs(Collections.singletonMap("retention.ms", "5"));
    adminClient.createTopics(Collections.singleton(topic)).all().get();
    System.out.println(topic);

    Producer<String, String> producer = testHelper.createProducer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    floodTopic(producer, binlogFilename, topicName);
    producer.close();

    TimeUnit.SECONDS.sleep(60);

    assertFalse(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 1L), topicName));
    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 10L), topicName));
  }

  @Test
  public void lookAtOldTopic() throws ExecutionException, InterruptedException {
    String topicName = "test_topic_1622213180385";
    String subscriberId = "duplicate-checker-" + topicName + "-" + System.currentTimeMillis();

    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 1L), topicName));
    assertTrue(duplicatePublishingDetector.shouldBePublished(new BinlogFileOffset(binlogFilename, 10L), topicName));
  }
*/

  @Test
  public void shouldHandlePublishCheckForOldEntries() {
    DuplicatePublishingDetector duplicatePublishingDetector = makeDuplicatePublishingDetector();

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
