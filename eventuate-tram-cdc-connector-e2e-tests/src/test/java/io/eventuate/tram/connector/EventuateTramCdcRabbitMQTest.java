package io.eventuate.tram.connector;

import com.rabbitmq.client.*;
import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.testutil.Eventually;
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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRabbitMQTest.Config.class})
public class EventuateTramCdcRabbitMQTest {

  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Value("${rabbitmq.url}")
  private String rabbitmqURL;

  private String subscriberId = UUID.randomUUID().toString();

  @Test
  public void insertToMessageTableAndWaitMessageInRabbitMQ() throws Exception {
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

  private void createConsumer(String channelName, Consumer<String> consumer) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbitmqURL);
    Connection connection = factory.newConnection();

    Channel subscriberGroupChannel = connection.createChannel();

    subscriberGroupChannel.exchangeDeclare(makeConsistentHashExchangeName(channelName, subscriberId), "x-consistent-hash");

    for (int i = 0; i < 2; i++) {
      subscriberGroupChannel.queueDeclare(makeConsistentHashQueueName(channelName, subscriberId, i), true, false, false, null);
      subscriberGroupChannel.queueBind(makeConsistentHashQueueName(channelName, subscriberId, i), makeConsistentHashExchangeName(channelName, subscriberId), "10");
    }

    subscriberGroupChannel.exchangeDeclare(channelName, "fanout");
    subscriberGroupChannel.exchangeBind(makeConsistentHashExchangeName(channelName, subscriberId), channelName, "");

    for (int i = 0; i < 2; i++) {
      String queue = makeConsistentHashQueueName(channelName, subscriberId, i);
      String exchange = makeConsistentHashExchangeName(channelName, subscriberId);

      Channel consumerChannel = connection.createChannel();

      consumerChannel.exchangeDeclare(exchange, "x-consistent-hash");
      consumerChannel.queueDeclare(queue, true, false, false, null);
      consumerChannel.queueBind(queue, exchange, "10");

      consumerChannel.basicConsume(queue, false, new DefaultConsumer(consumerChannel) {
        @Override
        public void handleDelivery(String consumerTag,
                                   Envelope envelope,
                                   AMQP.BasicProperties properties,
                                   byte[] body) throws IOException {
          String message = new String(body, "UTF-8");
          consumer.accept(message);
        }
      });
    }
  }

  private String makeConsistentHashExchangeName(String channelName, String subscriberId) {
    return String.format("%s-%s", channelName, subscriberId);
  }

  private String makeConsistentHashQueueName(String channelName, String subscriberId, int partition) {
    return String.format("%s-%s-%s", channelName, subscriberId, partition);
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
