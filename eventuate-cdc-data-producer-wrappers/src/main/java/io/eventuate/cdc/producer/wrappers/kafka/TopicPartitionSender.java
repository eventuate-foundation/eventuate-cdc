package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageKeyValue;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class TopicPartitionSender {
  private EventuateKafkaProducer eventuateKafkaProducer;
  private ConcurrentLinkedQueue<TopicPartitionMessage> messages = new ConcurrentLinkedQueue<>();
  private AtomicReference<TopicPartitionSenderState> state = new AtomicReference<>(TopicPartitionSenderState.IDLE);
  private boolean enableBatchProcessing;
  private int batchSize;

  public TopicPartitionSender(EventuateKafkaProducer eventuateKafkaProducer, boolean enableBatchProcessing, int batchSize) {
    this.eventuateKafkaProducer = eventuateKafkaProducer;
    this.enableBatchProcessing = enableBatchProcessing;
    this.batchSize = batchSize;
  }

  public CompletableFuture<?> sendMessage(String topic, String key, String body) {

    TopicPartitionMessage topicPartitionMessage = new TopicPartitionMessage(topic, key, body);

    if (state.get() == TopicPartitionSenderState.ERROR) {
      throw new RuntimeException("Sender is in error state, publishing is not possible.");
    }
    else {
      messages.add(topicPartitionMessage);

      if (state.compareAndSet(TopicPartitionSenderState.IDLE, TopicPartitionSenderState.SENDING)) {
        sendMessage();
      }
    }

    return topicPartitionMessage.getFuture();
  }

  private void sendMessage() {
    TopicPartitionMessage message = messages.poll();

    sendMessage(message);
  }

  private void sendMessage(TopicPartitionMessage message) {
    if (message != null) {

      eventuateKafkaProducer
              .send(message.getTopic(), message.getKey(), processBatchIfNecessary(message))
              .whenComplete(handleNextMessage(message));
    }
    else {
      state.set(TopicPartitionSenderState.IDLE);

      /*
        Additional check is necessary because,

        there can be the case when we got null message and not yet turned state to IDLE.
        But other thread added message to queue (just after we got null message) and tried to turn state to SENDING.
        Because state is not yet IDLE, 'sendMessage' will not be invoked.
        Then state will be turned to IDLE.
        So message will stay in queue until next 'sendMessage' invocation.
      */

      if (!messages.isEmpty() && state.compareAndSet(TopicPartitionSenderState.IDLE, TopicPartitionSenderState.SENDING)) {
        sendMessage();
      }
    }
  }


  private byte[] processBatchIfNecessary(TopicPartitionMessage message) {
    if (!enableBatchProcessing) {
      return message.getBody().getBytes(Charset.forName("UTF-8"));
    }

    List<TopicPartitionMessage> batch = new ArrayList<>();

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(batchSize);

    if (!messageBuilder.addMessage(new EventuateKafkaMultiMessageKeyValue(message.getKey(), message.getBody()))) {
      throw new RuntimeException("Cannot send kafka message, payload is too heavy!");
    }

    while (true) {
      TopicPartitionMessage messageForBatch = messages.peek();

      if (messageForBatch != null && messageBuilder
              .addMessage(new EventuateKafkaMultiMessageKeyValue(messageForBatch.getKey(), messageForBatch.getBody()))) {

        batch.add(messages.poll());
      }
      else {
        break;
      }
    }

    message.setBatch(batch);

    return messageBuilder.toBinaryArray();
  }

  private BiConsumer<Object, Throwable> handleNextMessage(TopicPartitionMessage previousMessage) {
    return (object, throwable) -> {
      if (throwable != null) {
        state.set(TopicPartitionSenderState.ERROR);
        previousMessage.getFuture().completeExceptionally(throwable);
        previousMessage.getBatch().stream().map(TopicPartitionMessage::getFuture).forEach(f -> f.completeExceptionally(throwable));
      } else {
        previousMessage.getFuture().complete(object);
        previousMessage.getBatch().stream().map(TopicPartitionMessage::getFuture).forEach(f -> f.complete(object));
        sendMessage();
      }
    };
  }
}
