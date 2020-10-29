package io.eventuate.local.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateLocalCdcTest.Config.class,
        KafkaConsumerFactoryConfiguration.class,
        SqlDialectConfiguration.class,
        IdGeneratorConfiguration.class})
public class EventuateLocalCdcTest extends AbstractEventuateCdcTest {

  @Import(EventuateKafkaPropertiesConfiguration.class)
  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  protected EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  protected KafkaConsumerFactory kafkaConsumerFactory;

  protected EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

  @Override
  protected void createConsumer(String topic, Consumer<String> consumer) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(subscriberId,
            (record, callback) -> {
              eventuateKafkaMultiMessageConverter.convertBytesToValues(record.value()).forEach(consumer);
              callback.accept(null, null);
              return null;
            },
            Collections.singletonList(topic),
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty(),
            kafkaConsumerFactory);

    eventuateKafkaConsumer.start();
  }

  @Override
  protected String saveEvent(String eventData, String entityType, EventuateSchema eventuateSchema, boolean published) {
    if (published) {
      return eventuateCommonJdbcOperations.insertPublishedEventIntoEventsTable(idGenerator,
              generateId(),
              eventData,
              generateId(),
              entityType,
              Optional.empty(),
              Optional.empty(),
              eventuateSchema);
    } else {
      return eventuateCommonJdbcOperations.insertIntoEventsTable(idGenerator,
              generateId(),
              eventData,
              generateId(),
              entityType,
              Optional.empty(),
              Optional.empty(),
              eventuateSchema);
    }
  }

  @Override
  protected String extractEventId(Map<String, Object> eventAsMap) {
    return (String) eventAsMap.get("id");
  }

  @Override
  protected String extractEventPayload(Map<String, Object> eventAsMap) {
    return (String) eventAsMap.get("eventData");
  }
}
