package io.eventuate.local.db.log.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.CompletableFutureUtil;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DatabaseOffsetKafkaStore extends OffsetKafkaStore {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private final String offsetStoreKey;

  private EventuateKafkaProducer eventuateKafkaProducer;

  public DatabaseOffsetKafkaStore(String dbHistoryTopicName,
                                  String offsetStoreKey,
                                  EventuateKafkaProducer eventuateKafkaProducer,
                                  EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties,
                                  EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {

    super(dbHistoryTopicName, eventuateKafkaConfigurationProperties, eventuateKafkaConsumerConfigurationProperties);

    this.offsetStoreKey = offsetStoreKey;
    this.eventuateKafkaProducer = eventuateKafkaProducer;
  }

  @Override
  public synchronized void save(BinlogFileOffset binlogFileOffset) {
      CompletableFuture<?> future =  eventuateKafkaProducer.send(
              dbHistoryTopicName,
              offsetStoreKey,
              JSonMapper.toJson(
                      binlogFileOffset
              )
      );

    CompletableFutureUtil.get(future);

    logger.debug("Offset is saved: {}", binlogFileOffset);
  }

  @Override
  protected BinlogFileOffset handleRecord(ConsumerRecord<String, String> record) {
    if (record.key().equals(offsetStoreKey)) {
      return JSonMapper.fromJson(record.value(), BinlogFileOffset.class);
    }
    return null;
  }
}
