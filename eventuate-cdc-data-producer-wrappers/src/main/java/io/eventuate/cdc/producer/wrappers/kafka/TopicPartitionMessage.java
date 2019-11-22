package io.eventuate.cdc.producer.wrappers.kafka;

import org.apache.commons.lang.builder.EqualsBuilder;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class TopicPartitionMessage {
  private String topic;
  private String key;
  private String body;

  private CompletableFuture<Object> future = new CompletableFuture<>();

  public TopicPartitionMessage(String topic, String key, String body) {
    this.topic = topic;
    this.key = key;
    this.body = body;
  }

  public String getTopic() {
    return topic;
  }

  public String getKey() {
    return key;
  }

  public String getBody() {
    return body;
  }

  public CompletableFuture<Object> getFuture() {
    return future;
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, key, body, future);
  }
}
