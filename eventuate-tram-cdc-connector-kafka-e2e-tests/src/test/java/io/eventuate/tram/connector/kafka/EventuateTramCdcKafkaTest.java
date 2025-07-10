package io.eventuate.tram.connector.kafka;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateCommonJdbcOperationsConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.consumer.OriginalTopicPartitionToSwimlaneMapping;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import io.eventuate.tram.connector.AbstractTramCdcTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.function.Consumer;

@SpringBootTest(classes = {EventuateTramCdcKafkaTest.Config.class})
public class EventuateTramCdcKafkaTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaPropertiesConfiguration.class,
          SqlDialectConfiguration.class,
          KafkaConsumerFactoryConfiguration.class, IdGeneratorConfiguration.class, EventuateCommonJdbcOperationsConfiguration.class})
  public static class Config {
  }

  @Autowired
  private EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Autowired
  private KafkaConsumerFactory kafkaConsumerFactory;

  @Override
  protected void createConsumer(String topic, Consumer<String> consumer) {
    MessageConsumerKafkaImpl messageConsumerKafka = new MessageConsumerKafkaImpl(eventuateKafkaConfigurationProperties.getBootstrapServers(),
            EventuateKafkaConsumerConfigurationProperties.empty(),
            kafkaConsumerFactory, new OriginalTopicPartitionToSwimlaneMapping());

    messageConsumerKafka.subscribe(subscriberId,
            ImmutableSet.of(topic),
            kafkaMessage -> consumer.accept(kafkaMessage.getPayload()));
  }
}
