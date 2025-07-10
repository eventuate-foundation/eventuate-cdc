package io.eventuate.local.db.log.test.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.db.log.common.DatabaseOffsetKafkaStore;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.TestHelperConfiguration;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = DatabaseOffsetKafkaStoreTest.Config.class)
@EnableAutoConfiguration
public class DatabaseOffsetKafkaStoreTest {

  @Configuration
  @Import({EventuateKafkaPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          SqlDialectConfiguration.class,
          IdGeneratorConfiguration.class,
          TestHelperConfiguration.class})
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
  public void shouldSendBinlogFilenameAndOffset() {
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
  public void shouldReadTheLastRecordMultipleTimes() {
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
