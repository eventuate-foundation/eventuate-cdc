package io.eventuate.tram.connector;

import io.eventuate.common.kafka.EventuateKafkaConfigurationProperties;
import io.eventuate.common.kafka.EventuateKafkaPropertiesConfiguration;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumer;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.testutil.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcKafkaTest.Config.class})
public class EventuateTramCdcKafkaTest {

  @Import(EventuateKafkaPropertiesConfiguration.class)
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Test
  public void insertToMessageTableAndWaitMessageInKafka() {
    String topic = UUID.randomUUID().toString();
    String data = UUID.randomUUID().toString();

    BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();

    createConsumer(topic, blockingQueue::add);

    saveEvent(data, topic, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));

    Eventually.eventually(120, 500, TimeUnit.MILLISECONDS, () -> {
      try {
        Assert.assertTrue(blockingQueue.poll(100, TimeUnit.MILLISECONDS).contains(data));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void createConsumer(String topic, Consumer<String> consumer) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(UUID.randomUUID().toString(),
            (record, callback) -> {
              consumer.accept(record.value());
              callback.accept(null, null);
            },
            Collections.singletonList(topic),
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    eventuateKafkaConsumer.start();
  }

  private void saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    String table = eventuateSchema.qualifyTable("message");
    String id = UUID.randomUUID().toString();

    jdbcTemplate.update(String.format("insert into %s(id, destination, headers, payload, creation_time) values(?, ?, ?, ?, ?)",
            table),
            id,
            eventType,
            String.format("{\"ID\" : \"%s\"}", id),
            eventData,
            System.currentTimeMillis());
  }
}
