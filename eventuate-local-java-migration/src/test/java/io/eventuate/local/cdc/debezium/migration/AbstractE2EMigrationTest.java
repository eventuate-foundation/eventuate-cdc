package io.eventuate.local.cdc.debezium.migration;

import io.eventuate.common.common.spring.jdbc.EventuateSpringJdbcStatementExecutor;
import io.eventuate.common.id.ApplicationIdGenerator;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateJdbcOperationsUtils;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.messaging.kafka.basic.consumer.*;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
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
  protected KafkaConsumerFactory kafkaConsumerFactory;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Value("spring.datasource.driver.class.name")
  private String driver;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  private EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  @Before
  public void init() {
    EventuateSqlDialect eventuateSqlDialect = sqlDialectSelector.getDialect(driver);

    eventuateCommonJdbcOperations = new EventuateCommonJdbcOperations(new EventuateJdbcOperationsUtils(eventuateSqlDialect),
            new EventuateSpringJdbcStatementExecutor(jdbcTemplate),
            eventuateSqlDialect);
  }

  protected void subscribe(Handler handler) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer("testSubscriber",
            handler,
            Collections.singletonList(aggregateType),
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty(),
            kafkaConsumerFactory);

    eventuateKafkaConsumer.start();

  }

  protected String sendEvent() {
    return eventuateCommonJdbcOperations.insertIntoEventsTable(new ApplicationIdGenerator(),
            generateId(),
            "",
            generateId(),
            aggregateType,
            Optional.empty(),
            Optional.empty(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }


  protected static class Handler implements EventuateKafkaConsumerMessageHandler {
    private BlockingQueue<String> events = new LinkedBlockingQueue<>();

    private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    @Override
    public MessageConsumerBacklog apply(ConsumerRecord<String, byte[]> record, BiConsumer<Void, Throwable> consumer) {
      events.addAll(eventuateKafkaMultiMessageConverter.convertBytesToValues(record.value()));
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
