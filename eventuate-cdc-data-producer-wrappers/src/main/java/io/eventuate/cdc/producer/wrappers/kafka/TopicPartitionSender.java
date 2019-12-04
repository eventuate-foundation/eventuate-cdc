package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageBuilder;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageKeyValue;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class TopicPartitionSender {
  private EventuateKafkaProducer eventuateKafkaProducer;
  private ConcurrentLinkedQueue<TopicPartitionMessage> messages = new ConcurrentLinkedQueue<>();
  private AtomicReference<TopicPartitionSenderState> state = new AtomicReference<>(TopicPartitionSenderState.IDLE);
  private TopicPartitionMessage lastFailedMessage;

  public TopicPartitionSender(EventuateKafkaProducer eventuateKafkaProducer) {
    this.eventuateKafkaProducer = eventuateKafkaProducer;
  }

  public CompletableFuture<?> sendMessage(String topic, String key, String body) {

    TopicPartitionMessage topicPartitionMessage = new TopicPartitionMessage(topic, key, body);

    /* if message future is completed exceptionally, it can be resent*/
    //Todo: now when batch is used logic should be changed. Remove for now?
    if (state.get() == TopicPartitionSenderState.ERROR && topicPartitionMessage.equals(lastFailedMessage)) {
      state.set(TopicPartitionSenderState.SENDING);
      sendMessage(topicPartitionMessage);
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

      List<TopicPartitionMessage> batch = new ArrayList<>();

      //Default size is 1MB, TODO: add configuration
      EventuateKafkaMultiMessageBuilder eventuateKafkaMultiMessageBuilder = new EventuateKafkaMultiMessageBuilder(1000000);

      if (!eventuateKafkaMultiMessageBuilder.addMessage(new EventuateKafkaMultiMessageKeyValue(message.getKey(), message.getBody()))) {
        throw new RuntimeException("Cannot send kafka message, payload is too heavy!");
      }

      while (true) {
        TopicPartitionMessage messageForBatch = messages.peek();

        if (messageForBatch != null && eventuateKafkaMultiMessageBuilder
                .addMessage(new EventuateKafkaMultiMessageKeyValue(messageForBatch.getKey(), messageForBatch.getBody()))) {

          batch.add(messages.poll());
        }
        else {
          break;
        }
      }

      message.setBatch(batch);

      eventuateKafkaProducer
              .send(message.getTopic(), message.getKey(), eventuateKafkaMultiMessageBuilder.toBinaryArray())
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

  private BiConsumer<Object, Throwable> handleNextMessage(TopicPartitionMessage previousMessage) {
    return (object, throwable) -> {
      if (throwable != null) {
        lastFailedMessage = previousMessage;
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
