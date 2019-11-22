package io.eventuate.cdc.producer.wrappers.kafka;

import org.apache.commons.lang.builder.EqualsBuilder;

import java.util.Objects;

public class TopicPartition {
  private String topic;
  private int partition;

  public TopicPartition(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }
}
