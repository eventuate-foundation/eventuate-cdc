package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.messaging.kafka.basic.consumer.*;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class DuplicatePublishingDetector implements PublishingFilter {

  private Logger logger = LoggerFactory.getLogger(getClass());
  private Map<String, Optional<BinlogFileOffset>> maxOffsetsForTopics = new HashMap<>();
  private boolean okToProcess = false;
  private String kafkaBootstrapServers;
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;
  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();
  private KafkaConsumerFactory kafkaConsumerFactory;

  public DuplicatePublishingDetector(String kafkaBootstrapServers,
                                     EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                     KafkaConsumerFactory kafkaConsumerFactory) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.eventuateKafkaConsumerConfigurationProperties = eventuateKafkaConsumerConfigurationProperties;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
  }

  @Override
  public boolean shouldBePublished(BinlogFileOffset sourceBinlogFileOffset, String destinationTopic) {
    if (okToProcess)
      return true;

    Optional<BinlogFileOffset> max = maxOffsetsForTopics.computeIfAbsent(destinationTopic, this::fetchMaxBinlogFileOffsetFor);
    logger.info("For topic {} max is {}", destinationTopic, max);

    okToProcess = max.map(sourceBinlogFileOffset::isSameOrAfter).orElse(true);

    logger.info("max = {}, sourceBinlogFileOffset = {} okToProcess = {}", max, sourceBinlogFileOffset, okToProcess);
    return okToProcess;
  }

  private Optional<BinlogFileOffset> fetchMaxBinlogFileOffsetFor(String destinationTopic) {
    String subscriberId = "duplicate-checker-" + destinationTopic + "-" + System.currentTimeMillis();
    Properties consumerProperties = ConsumerPropertiesFactory.makeDefaultConsumerProperties(kafkaBootstrapServers, subscriberId);
    consumerProperties.putAll(eventuateKafkaConsumerConfigurationProperties.getProperties());

    KafkaMessageConsumer consumer = kafkaConsumerFactory.makeConsumer(null, consumerProperties);

    logger.info("fetching maxOffsetFor {}", subscriberId);

    List<PartitionInfo> partitions = EventuateKafkaConsumer.verifyTopicExistsBeforeSubscribing(consumer, destinationTopic);

    List<TopicPartition> topicPartitionList = partitions.stream().map(p -> new TopicPartition(destinationTopic, p.partition())).collect(toList());
    consumer.assign(topicPartitionList);
    consumer.poll(Duration.ZERO);

    List<ConsumerRecord<String, byte[]>> records = getLastRecords(consumer, topicPartitionList);
    consumer.close();

    return getMaxBinlogFileOffsetFromRecords(records);
  }

  public List<ConsumerRecord<String, byte[]>> getLastRecords(KafkaMessageConsumer consumer, List<TopicPartition> topicPartitionList) {

    List<PartitionOffset> offsetsOfLastRecords = getOffsetsOfLastRecords(consumer, topicPartitionList);

    logger.info("Seeking to offsetsOfLastRecords={}, {}", topicPartitionList, offsetsOfLastRecords);

    offsetsOfLastRecords.forEach(po -> {
      consumer.seek(new TopicPartition(topicPartitionList.get(0).topic(), po.partition), po.offset);
    });

    int expectedRecordCount = offsetsOfLastRecords.size();
    return getLastRecords(consumer, topicPartitionList, expectedRecordCount);
  }

  private List<PartitionOffset> getOffsetsOfLastRecords(KafkaMessageConsumer consumer, List<TopicPartition> topicPartitionList) {

    Map<TopicPartition, Long> beginningOffsets;
    Map<TopicPartition, Long> endOffsets;

    try {
      KafkaConsumer<String, byte[]> kc = getActualKafkaConsumerHack(consumer);
      beginningOffsets = kc.beginningOffsets(topicPartitionList);
      endOffsets = kc.endOffsets(topicPartitionList);
      logger.info("for {} beginning {}, ending {}", topicPartitionList, beginningOffsets, endOffsets);
    } catch (IllegalStateException e) {
      throw new RuntimeException("Error getting offsets " + topicPartitionList, e);
    }

    // Ignore partitions where the beginning == end since that means they are empty

    return endOffsets.entrySet().stream()
            .filter( endOffset -> !isEmptyPartition(beginningOffsets.get(endOffset.getKey()), endOffset.getValue()))
            .map(endOffset -> new PartitionOffset(endOffset.getKey().partition(), endOffset.getValue() - 1))
            .collect(toList());
  }

  private boolean isEmptyPartition(Long beginningOffset, Long endOffset) {
    return endOffset.equals(beginningOffset);
  }

  private KafkaConsumer<String, byte[]> getActualKafkaConsumerHack(KafkaMessageConsumer consumer) {
    Field delegateField = ReflectionUtils.findField(consumer.getClass(), "delegate");
    delegateField.setAccessible(true);
    return (KafkaConsumer<String, byte[]>) ReflectionUtils.getField(delegateField, consumer);
  }

  private List<ConsumerRecord<String, byte[]>> getLastRecords(KafkaMessageConsumer consumer, List<TopicPartition> topicPartitionList, int expectedRecordCount) {
    logger.info("Getting last records: {}", topicPartitionList);

    List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
    while (records.size() < expectedRecordCount) {
      ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
      consumerRecords.forEach(records::add);

      logger.info("Got some last records: {} {}", topicPartitionList, consumerRecords.count());
    }

    logger.info("Got all last records: {} {}", topicPartitionList, records.size());

    return records;
  }

  private Optional<BinlogFileOffset> getMaxBinlogFileOffsetFromRecords(List<ConsumerRecord<String, byte[]>> records) {
    return records
            .stream()
            .flatMap(record -> {
              logger.info("Got record: {}, {}, {}", record.topic(), record.partition(), record.offset());

              return eventuateKafkaMultiMessageConverter
                      .convertBytesToValues(record.value())
                      .stream()
                      .map(value -> JSonMapper.fromJson(value, PublishedEvent.class).getBinlogFileOffset());

            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .max((blfo1, blfo2) -> blfo1.isSameOrAfter(blfo2) ? 1 : -1);
  }

  class PartitionOffset {

    public final int partition;
    public final long offset;

    @Override
    public String toString() {
      return "PartitionOffset{" +
              "partition=" + partition +
              ", offset=" + offset +
              '}';
    }

    public PartitionOffset(int partition, long offset) {

      this.partition = partition;
      this.offset = offset;
    }
  }
}
