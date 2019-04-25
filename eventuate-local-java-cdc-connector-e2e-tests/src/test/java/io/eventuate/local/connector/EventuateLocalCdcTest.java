package io.eventuate.local.connector;

import io.eventuate.Int128;
import io.eventuate.common.kafka.EventuateKafkaConfigurationProperties;
import io.eventuate.common.kafka.EventuateKafkaPropertiesConfiguration;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumer;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.javaclient.commonimpl.EntityIdVersionAndEventIds;
import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.javaclient.spring.jdbc.IdGenerator;
import io.eventuate.javaclient.spring.jdbc.IdGeneratorImpl;
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
@SpringBootTest(classes = {EventuateLocalCdcTest.Config.class})
public class EventuateLocalCdcTest {

  @Import(EventuateKafkaPropertiesConfiguration.class)
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  protected EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  private IdGenerator idGenerator = new IdGeneratorImpl();

  @Test
  public void insertToEventTableAndWaitEventInKafka() {
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

  private EntityIdVersionAndEventIds saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    Int128 eventId = idGenerator.genId();
    String entityId = idGenerator.genId().asString();

    jdbcTemplate.update(String.format("INSERT INTO %s (event_id, event_type, event_data, entity_type, entity_id, triggering_event, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)", eventuateSchema.qualifyTable("events")),
            eventId.asString(), eventType, eventData, eventType, entityId,
            null,
            null);

    return new EntityIdVersionAndEventIds(entityId, eventId, Collections.singletonList(eventId));
  }
}
