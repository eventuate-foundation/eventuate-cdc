package io.eventuate.local.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaPropertiesConfiguration;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateLocalCdcTest.Config.class})
public class EventuateLocalCdcTest extends AbstractEventuateCdcTest {

  @Import(EventuateKafkaPropertiesConfiguration.class)
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  protected EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Override
  protected void createConsumer(String topic, Consumer<String> consumer) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(subscriberId,
            (record, callback) -> {
              consumer.accept(record.value());
              callback.accept(null, null);
            },
            Collections.singletonList(topic),
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty());

    eventuateKafkaConsumer.start();
  }

  @Override
  protected void saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema) {
    eventuateCommonJdbcOperations.insertIntoEventsTable(generateId(),
            generateId(),
            eventData,
            generateId(),
            entityType,
            Optional.empty(),
            Optional.empty(),
            eventuateSchema);
  }
}
