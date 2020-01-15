package io.eventuate.cdc.producer.wrappers.kafka;

import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageKeyValue;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TopicPartitionSender {
  private EventuateKafkaProducer eventuateKafkaProducer;
  private ConcurrentLinkedQueue<TopicPartitionMessage> messages = new ConcurrentLinkedQueue<>();
  private AtomicReference<TopicPartitionSenderState> state = new AtomicReference<>(TopicPartitionSenderState.IDLE);
  private boolean enableBatchProcessing;
  private int batchSize;
  private MeterRegistry meterRegistry;
  private AtomicLong timeOfLastProcessedMessage;

  public TopicPartitionSender(EventuateKafkaProducer eventuateKafkaProducer,
                              boolean enableBatchProcessing,
                              int batchSize,
                              MeterRegistry meterRegistry) {

    this.eventuateKafkaProducer = eventuateKafkaProducer;
    this.enableBatchProcessing = enableBatchProcessing;
    this.batchSize = batchSize;
    this.meterRegistry = meterRegistry;
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
    if (!messages.isEmpty()) {
      sendNextMessage();
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

  private void sendNextMessage() {
    if (enableBatchProcessing) {
      sendMessageBatch();
    }
    else {
      sendSingleMessage();
    }
  }

  private void sendSingleMessage() {
    TopicPartitionMessage topicPartitionMessage;

    while ((topicPartitionMessage = messages.poll()) != null) {
      TopicPartitionMessage message = topicPartitionMessage;

      eventuateKafkaProducer
        .send(message.getTopic(), message.getKey(), EventuateBinaryMessageEncoding.stringToBytes(message.getBody()))
        .whenComplete((o, throwable) -> {
          updateMetrics(1);
          if (throwable != null) {
            state.set(TopicPartitionSenderState.ERROR);
            message.completeExceptionally(throwable);
          }
          else {
            message.complete(o);
            sendMessage();
          }
        });
    }
  }

  private void sendMessageBatch() {
    List<TopicPartitionMessage> batch = new ArrayList<>();
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(batchSize);

    String key = null;
    String topic = null;


    while (true) {
      TopicPartitionMessage messageForBatch = messages.peek();

      if (messageForBatch != null &&
              messageBuilder.addMessage(new EventuateKafkaMultiMessageKeyValue(messageForBatch.getKey(), messageForBatch.getBody()))) {

        messageForBatch = messages.poll();

        //key of the first message is a kafka record key
        if (key == null) {
          key = messageForBatch.getKey();
          topic = messageForBatch.getTopic();
        }

        batch.add(messageForBatch);
      }
      else {
        break;
      }
    }

    if (batch.isEmpty()) {
      state.set(TopicPartitionSenderState.ERROR);
      throw new RuntimeException("Message is too big to send.");
    }

    eventuateKafkaProducer
            .send(topic, key, messageBuilder.toBinaryArray())
            .whenComplete((o, throwable) -> {
              updateMetrics(batch.size());
              if (throwable != null) {
                state.set(TopicPartitionSenderState.ERROR);
                batch.forEach(m -> m.completeExceptionally(throwable));
              }
              else {
                batch.forEach(m -> m.complete(o));
                sendMessage();
              }
            });
  }

  private void updateMetrics(int processedEvents) {
    meterRegistry.counter("eventuate.cdc.processed.messages").increment(processedEvents);
    if (timeOfLastProcessedMessage == null) {
      timeOfLastProcessedMessage = new AtomicLong(System.nanoTime());
      meterRegistry.gauge("eventuate.cdc.time.of.last.processed.message", timeOfLastProcessedMessage);
    } else {
      timeOfLastProcessedMessage.set(System.nanoTime());
    }
  }
}
