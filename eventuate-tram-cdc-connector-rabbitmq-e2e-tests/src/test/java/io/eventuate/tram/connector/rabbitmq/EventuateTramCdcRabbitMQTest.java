package io.eventuate.tram.connector.rabbitmq;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateCommonJdbcOperationsConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.rabbitmq.spring.consumer.MessageConsumerRabbitMQConfiguration;
import io.eventuate.messaging.rabbitmq.spring.consumer.MessageConsumerRabbitMQImpl;
import io.eventuate.tram.connector.AbstractTramCdcTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.function.Consumer;

@SpringBootTest(classes = {EventuateTramCdcRabbitMQTest.Config.class})
public class EventuateTramCdcRabbitMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({SqlDialectConfiguration.class, MessageConsumerRabbitMQConfiguration.class, IdGeneratorConfiguration.class, EventuateCommonJdbcOperationsConfiguration.class})
  public static class Config {}

  @Autowired
  private MessageConsumerRabbitMQImpl messageConsumerRabbitMQ;

  @Override
  protected void createConsumer(String channelName, Consumer<String> consumer) {
    messageConsumerRabbitMQ.subscribe(subscriberId,
            ImmutableSet.of(channelName),
            rabbitMQMessage -> consumer.accept(rabbitMQMessage.getPayload()));
  }
}
