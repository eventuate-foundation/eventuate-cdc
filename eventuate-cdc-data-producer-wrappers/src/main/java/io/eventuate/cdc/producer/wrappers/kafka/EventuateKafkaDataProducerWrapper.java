package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class EventuateKafkaDataProducerWrapper implements DataProducer {

  private EventuateKafkaProducer eventuateKafkaProducer;
  private final ConcurrentHashMap<TopicPartition, TopicPartitionSender> topicPartitionSenders = new ConcurrentHashMap<>();

  public EventuateKafkaDataProducerWrapper(EventuateKafkaProducer eventuateKafkaProducer) {
    this.eventuateKafkaProducer = eventuateKafkaProducer;
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return getOrCreateTopicPartitionSender(topic, key).handleEvent(topic, key, body);
  }

  @Override
  public void close() {
    eventuateKafkaProducer.close();
  }

  private TopicPartitionSender getOrCreateTopicPartitionSender(String topic, String key) {

    TopicPartition topicPartition = new TopicPartition(topic, eventuateKafkaProducer.partitionFor(topic, key));

    return topicPartitionSenders.computeIfAbsent(topicPartition, tp -> new TopicPartitionSender(eventuateKafkaProducer));
  }
}
