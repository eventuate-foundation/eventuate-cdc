package io.eventuate.cdc.producer.wrappers;

import java.util.concurrent.CompletableFuture;

public interface DataProducer {
  CompletableFuture<?> send(String topic, String key, String body);
  void close();
}