package io.eventuate.tram.connector.rabbitmq;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.rabbitmq.spring.consumer.MessageConsumerRabbitMQConfiguration;
import io.eventuate.messaging.rabbitmq.spring.consumer.MessageConsumerRabbitMQImpl;
import io.eventuate.tram.connector.AbstractTramCdcTest;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRabbitMQTest.Config.class, IdGeneratorConfiguration.class})
public class EventuateTramCdcRabbitMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({SqlDialectConfiguration.class, MessageConsumerRabbitMQConfiguration.class})
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
