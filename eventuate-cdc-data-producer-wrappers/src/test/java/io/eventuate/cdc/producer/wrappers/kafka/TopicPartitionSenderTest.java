package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.local.test.util.TestHelper;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TopicPartitionSenderTest.Config.class)
public class TopicPartitionSenderTest {

  @Configuration
  public static class Config {}

  @Value("${eventuatelocal.kafka.bootstrap.servers}")
  private String kafkaBootstrapServers;

  private TestHelper testHelper = new TestHelper();

  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

  private final int nEvents = 1000;
  private String topic;
  private String key;

  @Before
  public void init() {
    topic = testHelper.generateId();
    key = testHelper.generateId();
  }

  @Test
  public void testBatchProcessing() throws InterruptedException {
    sendEvents();
    assertAllMessagesReceived(receiveEvents());
  }

  private void sendEvents() throws InterruptedException {
    TopicPartitionSender topicPartitionSender = new TopicPartitionSender(
            new EventuateKafkaProducer(kafkaBootstrapServers, EventuateKafkaProducerConfigurationProperties.empty()),
            true,
            1000000,
            new LoggingMeterRegistry());

    for (int i = 0; i < nEvents; i++) {
      int fi = i;
      topicPartitionSender.sendMessage(topic, key, String.valueOf(fi));
      if (i == nEvents / 2) {
        Thread.sleep(1000);
      }
    }
  }

  private List<EventuateKafkaMultiMessage> receiveEvents() {
    try (KafkaConsumer<String, byte[]> consumer = testHelper.createConsumer(kafkaBootstrapServers)) {
      consumer.subscribe(Collections.singletonList(topic));

      ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(30000);

      List<EventuateKafkaMultiMessage> messages = new ArrayList<>();

      for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
        messages.addAll(eventuateKafkaMultiMessageConverter.convertBytesToMessages(consumerRecord.value()).getMessages());
      }

      return messages;
    }
  }

  private void assertAllMessagesReceived(List<EventuateKafkaMultiMessage> messages) {
    for (int i = 0; i < nEvents; i++) {
      EventuateKafkaMultiMessage message = messages.get(i);
      Assert.assertEquals(key, message.getKey());
      Assert.assertEquals(String.valueOf(i), message.getValue());
    }
  }
}
