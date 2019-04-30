package io.eventuate.tram.connector;

import io.eventuate.sql.dialect.SqlDialectConfiguration;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.*;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcActiveMQTest.Config.class})
public class EventuateTramCdcActiveMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import(SqlDialectConfiguration.class)
  public static class Config {
  }

  @Value("${activemq.url}")
  private String activeMQUrl;

  @Override
  protected void createConsumer(String channel, Consumer<String> javaConsumer)  throws Exception {
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(activeMQUrl);
    Connection connection = connectionFactory.createConnection();
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    String destinationName = String.format("Consumer.%s.VirtualTopic.%s", subscriberId, channel);

    Destination destination = session.createQueue(destinationName);

    javax.jms.MessageConsumer consumer = session.createConsumer(destination);

    new Thread(() -> {
      try {
        while (true) {
          javax.jms.Message message = consumer.receive(100);

          if (message == null) {
            continue;
          }

          javaConsumer.accept(((TextMessage) message).getText());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).start();
  }
}
