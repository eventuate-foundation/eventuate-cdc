package io.eventuate.cdc.producer.wrappers.kafka;

public enum TopicPartitionSenderState {
  IDLE, SENDING, ERROR
}
