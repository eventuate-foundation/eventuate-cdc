package io.eventuate.cdc.producer.wrappers.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class TopicPartitionMessage {
  private String topic;
  private String key;
  private String body;
  private List<TopicPartitionMessage> batch = Collections.emptyList();

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
    if (o == null) return false;
    if (!(o instanceof TopicPartitionMessage)) return false;
    TopicPartitionMessage tpm = (TopicPartitionMessage) o;

    return Objects.equals(topic, tpm.getTopic()) && Objects.equals(key, tpm.getKey()) && Objects.equals(body, tpm.getBody());
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, key, body);
  }

  public List<TopicPartitionMessage> getBatch() {
    return batch;
  }

  public void setBatch(List<TopicPartitionMessage> messages) {
    this.batch = messages;
  }
}
