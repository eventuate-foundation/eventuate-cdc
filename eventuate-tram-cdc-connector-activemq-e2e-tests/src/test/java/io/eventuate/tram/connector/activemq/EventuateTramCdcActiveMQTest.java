package io.eventuate.tram.connector.activemq;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.activemq.spring.common.EventuateActiveMQCommonConfiguration;
import io.eventuate.messaging.activemq.spring.common.EventuateActiveMQConfigurationProperties;
import io.eventuate.messaging.activemq.spring.consumer.MessageConsumerActiveMQImpl;
import io.eventuate.tram.connector.AbstractTramCdcTest;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcActiveMQTest.Config.class,
        EventuateActiveMQCommonConfiguration.class})
public class EventuateTramCdcActiveMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import(SqlDialectConfiguration.class)
  public static class Config {
  }

  @Autowired
  private EventuateActiveMQConfigurationProperties eventuateActiveMQConfigurationProperties;

  @Override
  protected void createConsumer(String channel, Consumer<String> javaConsumer) {
    MessageConsumerActiveMQImpl messageConsumerActiveMQ = new MessageConsumerActiveMQImpl(eventuateActiveMQConfigurationProperties.getUrl(),
      Optional.ofNullable(eventuateActiveMQConfigurationProperties.getUser()),
      Optional.ofNullable(eventuateActiveMQConfigurationProperties.getPassword()));

    messageConsumerActiveMQ.subscribe(subscriberId,
            ImmutableSet.of(channel),
            activeMQMessage -> javaConsumer.accept(activeMQMessage.getPayload()));
  }
}
