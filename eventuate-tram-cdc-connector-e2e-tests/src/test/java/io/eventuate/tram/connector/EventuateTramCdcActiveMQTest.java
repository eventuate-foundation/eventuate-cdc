package io.eventuate.tram.connector;

import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.testutil.Eventually;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.*;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcActiveMQTest.Config.class})
public class EventuateTramCdcActiveMQTest {

  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Value("${activemq.url}")
  private String activeMQUrl;

  private String subscriberId = UUID.randomUUID().toString();

  @Test
  public void insertToMessageTableAndWaitMessageInActiveMQ() throws Exception{
    String topic = UUID.randomUUID().toString();
    String data = UUID.randomUUID().toString();

    BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();

    createConsumer(topic, blockingQueue::add);

    saveEvent(data, topic, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));

    Eventually.eventually(120, 500, TimeUnit.MILLISECONDS, () -> {
      try {
        Assert.assertTrue(blockingQueue.poll(100, TimeUnit.MILLISECONDS).contains(data));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void createConsumer(String channel, Consumer<String> javaConsumer)  throws Exception {
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

  private void saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    String table = eventuateSchema.qualifyTable("message");
    String id = UUID.randomUUID().toString();

    jdbcTemplate.update(String.format("insert into %s(id, destination, headers, payload, creation_time) values(?, ?, ?, ?, ?)",
            table),
            id,
            eventType,
            String.format("{\"ID\" : \"%s\"}", id),
            eventData,
            System.currentTimeMillis());
  }
}
