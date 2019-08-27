package io.eventuate.local.cdc.debezium.migration;

import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerMessageHandler;
import io.eventuate.messaging.kafka.basic.consumer.MessageConsumerBacklog;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public abstract class AbstractE2EMigrationTest {

  private static final String aggregateType = "TestAggregate_MIGRATION";

  @Autowired
  protected EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  private EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  @Before
  public void init() {
    eventuateCommonJdbcOperations = new EventuateCommonJdbcOperations(jdbcTemplate);
  }

  protected void subscribe(Handler handler) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer("testSubscriber",
            handler,
            Collections.singletonList(aggregateType),
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    eventuateKafkaConsumer.start();

  }

  protected String sendEvent() {
    String id = UUID.randomUUID().toString();

    eventuateCommonJdbcOperations.insertIntoEventsTable(id,
            generateId(),
            "",
            generateId(),
            aggregateType,
            Optional.empty(),
            Optional.empty(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));

    return id;
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }


  protected static class Handler implements EventuateKafkaConsumerMessageHandler {
    private BlockingQueue<String> events = new LinkedBlockingQueue<>();

    @Override
    public MessageConsumerBacklog apply(ConsumerRecord<String, String> record, BiConsumer<Void, Throwable> consumer) {
      events.add(record.value());
      consumer.accept(null, null);
      return null;
    }

    public void assertContainsEvent() throws InterruptedException {
      Assert.assertNotNull(events.poll(60, TimeUnit.SECONDS));
    }

    public void assertContainsEventWithId(String id) throws InterruptedException {
      Assert.assertTrue(events.poll(60, TimeUnit.SECONDS).contains(id));
    }
  }
}
