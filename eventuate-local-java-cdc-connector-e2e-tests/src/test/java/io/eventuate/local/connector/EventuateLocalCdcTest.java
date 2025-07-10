package io.eventuate.local.connector;

import io.eventuate.cdc.e2e.common.AbstractEventuateCdcTest;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateCommonJdbcOperationsConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@SpringBootTest(classes = {EventuateLocalCdcTest.Config.class})
public class EventuateLocalCdcTest extends AbstractEventuateCdcTest {

  @Import({EventuateKafkaPropertiesConfiguration.class,
          KafkaConsumerFactoryConfiguration.class,
          SqlDialectConfiguration.class,
          IdGeneratorConfiguration.class,
          EventuateCommonJdbcOperationsConfiguration.class})
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
