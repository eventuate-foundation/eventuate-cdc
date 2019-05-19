package io.eventuate.tram.connector;

import io.eventuate.common.kafka.EventuateKafkaConfigurationProperties;
import io.eventuate.common.kafka.EventuateKafkaPropertiesConfiguration;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumer;
import io.eventuate.common.kafka.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.sql.dialect.SqlDialectConfiguration;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcKafkaTest.Config.class})
public class EventuateTramCdcKafkaTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaPropertiesConfiguration.class, SqlDialectConfiguration.class})
  public static class Config {
  }

  @Autowired
  private EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  @Override
  protected void createConsumer(String topic, Consumer<String> consumer) throws Exception {
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
}
