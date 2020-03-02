package io.eventuate.local.db.log.test.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.db.log.common.DatabaseOffsetKafkaStore;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
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
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = DatabaseOffsetKafkaStoreTest.Config.class)
@EnableAutoConfiguration
public class DatabaseOffsetKafkaStoreTest {

  @Configuration
  @Import({EventuateKafkaPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class})
  public static class Config {
    @Bean
    public EventuateConfigurationProperties eventuateConfigurationProperties() {
      return new EventuateConfigurationProperties();
    }

    @Bean
    public EventuateKafkaProducer eventuateKafkaProducer(EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                                         EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties) {
      return new EventuateKafkaProducer(eventuateKafkaConfigurationProperties.getBootstrapServers(),
              eventuateKafkaProducerConfigurationProperties);
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
  EventuateKafkaProducer eventuateKafkaProducer;

  @Autowired
  EventuateConfigurationProperties eventuateConfigurationProperties;

  @Autowired
  EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  TestHelper testHelper;

  @Test
  public void shouldSendBinlogFilenameAndOffset() throws InterruptedException {
    generateAndSaveBinlogFileOffset();
  }

  @Test
  public void shouldGetEmptyOptionalFromEmptyTopic() {
    OffsetStore offsetStore = getDatabaseOffsetKafkaStore(UUID.randomUUID().toString(), "mySqlBinaryLogClientName");
    offsetStore.getLastBinlogFileOffset().isPresent();
  }

  @Test
  public void shouldWorkCorrectlyWithMultipleDifferentNamedBinlogs() {
    floodTopic(eventuateConfigurationProperties.getOffsetStorageTopicName(), "mySqlBinaryLogClientName1");

    generateAndSaveBinlogFileOffset();
  }

  @Test
  public void shouldReadTheLastRecordMultipleTimes() throws InterruptedException {
    long t = System.currentTimeMillis();
    BinlogFileOffset bfo = generateAndSaveBinlogFileOffset();

    System.out.println((System.currentTimeMillis() - t) / 1000);

    assertLastRecordEquals(bfo);
    System.out.println((System.currentTimeMillis() - t) / 1000);
    assertLastRecordEquals(bfo);
    System.out.println((System.currentTimeMillis() - t) / 1000);
  }

  private void floodTopic(String topicName, String key) {
    Producer<String, String> producer = testHelper.createProducer(eventuateKafkaConfigurationProperties.getBootstrapServers());
    for (int i = 0; i < 10; i++)
      producer.send(new ProducerRecord<>(topicName, key, Integer.toString(i)));

    producer.close();
  }

  public DatabaseOffsetKafkaStore getDatabaseOffsetKafkaStore(String topicName, String key) {
    return new DatabaseOffsetKafkaStore(topicName,
            key,
            eventuateKafkaProducer,
            eventuateKafkaConfigurationProperties,
            EventuateKafkaConsumerConfigurationProperties.empty());
  }

  private BinlogFileOffset generateAndSaveBinlogFileOffset() {
    BinlogFileOffset bfo = testHelper.generateBinlogFileOffset();
    DatabaseOffsetKafkaStore offsetStore = getDatabaseOffsetKafkaStore(eventuateConfigurationProperties.getOffsetStorageTopicName(), "mySqlBinaryLogClientName");
    offsetStore.save(bfo);

    BinlogFileOffset savedBfo = offsetStore.getLastBinlogFileOffset().get();
    assertEquals(bfo, savedBfo);
    return savedBfo;
  }

  private void assertLastRecordEquals(BinlogFileOffset binlogFileOffset) {
    OffsetStore offsetStore = getDatabaseOffsetKafkaStore(eventuateConfigurationProperties.getOffsetStorageTopicName(), "mySqlBinaryLogClientName");

    BinlogFileOffset lastRecord = offsetStore.getLastBinlogFileOffset().get();
    assertEquals(binlogFileOffset, lastRecord);
  }
}
