package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.cdc.producer.wrappers.DataProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class EventuateKafkaDataProducerWrapper implements DataProducer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private EventuateKafkaProducer eventuateKafkaProducer;
  private final ConcurrentHashMap<TopicPartition, TopicPartitionSender> topicPartitionSenders = new ConcurrentHashMap<>();
  private boolean enableBatchProcessing;
  private int batchSize;
  private MeterRegistry meterRegistry;

  public EventuateKafkaDataProducerWrapper(EventuateKafkaProducer eventuateKafkaProducer,
                                           boolean enableBatchProcessing,
                                           int batchSize,
                                           MeterRegistry meterRegistry) {
    this.eventuateKafkaProducer = eventuateKafkaProducer;
    this.enableBatchProcessing = enableBatchProcessing;
    this.batchSize = batchSize;
    this.meterRegistry = meterRegistry;

    logger.info("enableBatchProcessing={}", enableBatchProcessing);
    logger.info("batchSize={}", batchSize);
  }

  @Override
  public CompletableFuture<?> send(String topic, String key, String body) {
    return getOrCreateTopicPartitionSender(topic, key, meterRegistry).sendMessage(topic, key, body);
  }

  @Override
  public void close() {
    eventuateKafkaProducer.close();
  }

  private TopicPartitionSender getOrCreateTopicPartitionSender(String topic, String key, MeterRegistry meterRegistry) {

    TopicPartition topicPartition = new TopicPartition(topic, eventuateKafkaProducer.partitionFor(topic, key));

    return topicPartitionSenders.computeIfAbsent(topicPartition,
            tp -> new TopicPartitionSender(eventuateKafkaProducer, enableBatchProcessing, batchSize, meterRegistry));
  }
}
