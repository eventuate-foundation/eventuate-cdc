package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;

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
      eventuateKafkaProducer.send(message.getTopic(), message.getKey(), message.getBody()).whenComplete(handleNextMessage(message));
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
      } else {
        previousMessage.getFuture().complete(object);
        sendMessage();
      }
    };
  }
}
