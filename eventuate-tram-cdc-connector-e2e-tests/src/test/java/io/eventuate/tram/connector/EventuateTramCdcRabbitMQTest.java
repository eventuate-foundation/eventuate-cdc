package io.eventuate.tram.connector;

import com.rabbitmq.client.*;
import io.eventuate.sql.dialect.SqlDialectConfiguration;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EventuateTramCdcRabbitMQTest.Config.class})
public class EventuateTramCdcRabbitMQTest extends AbstractTramCdcTest {

  @Configuration
  @EnableAutoConfiguration
  @Import(SqlDialectConfiguration.class)
  public static class Config {
  }

  @Value("${rabbitmq.url}")
  private String rabbitmqURL;

  @Override
  protected void createConsumer(String channelName, Consumer<String> consumer) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbitmqURL);
    Connection connection = factory.newConnection();

    Channel subscriberGroupChannel = connection.createChannel();

    subscriberGroupChannel.exchangeDeclare(makeConsistentHashExchangeName(channelName, subscriberId), "x-consistent-hash");

    subscriberGroupChannel.queueDeclare(makeConsistentHashQueueName(channelName, subscriberId, 0), true, false, false, null);
    subscriberGroupChannel.queueBind(makeConsistentHashQueueName(channelName, subscriberId, 0), makeConsistentHashExchangeName(channelName, subscriberId), "10");

    subscriberGroupChannel.exchangeDeclare(channelName, "fanout");
    subscriberGroupChannel.exchangeBind(makeConsistentHashExchangeName(channelName, subscriberId), channelName, "");

    String queue = makeConsistentHashQueueName(channelName, subscriberId, 0);
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

  private String makeConsistentHashExchangeName(String channelName, String subscriberId) {
    return String.format("%s-%s", channelName, subscriberId);
  }

  private String makeConsistentHashQueueName(String channelName, String subscriberId, int partition) {
    return String.format("%s-%s-%s", channelName, subscriberId, partition);
  }
}
