package io.eventuate.local.test.util;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.BinlogEntryToEventConverter;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TestHelper {

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private IdGenerator idGenerator;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  @Autowired(required = false)
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  protected EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();


  public String getEventTopicName() {
    return "TestEntity";
  }

  public String getTestEntityType() {
    return "TestEntity";
  }

  public String getTestCreatedEventType() {
    return "TestCreatedEvent";
  }

  public String getTestUpdatedEventType() {
    return "TestUpdatedEvent";
  }

  public String generateUniqueTopicName() {
    return "test_topic_" + System.currentTimeMillis();
  }

  public void runInSeparateThread(Runnable callback) {
    new Thread(callback).start();
  }

  public BinlogFileOffset generateBinlogFileOffset() {
    long now = System.currentTimeMillis();
    return new BinlogFileOffset("binlog.filename." + now, now);
  }

  public Producer<String, String> createProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  public KafkaConsumer<String, byte[]> createConsumer(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("auto.offset.reset", "earliest");
    props.put("group.id", UUID.randomUUID().toString());
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    return new KafkaConsumer<>(props);
  }

  public EventInfo saveRandomEvent() {
    return saveEvent(getTestEntityType(), getTestCreatedEventType(), generateId(), eventuateSchema);
  }

  public EventInfo saveEvent(String entityType, String eventType, String eventData, EventuateSchema eventuateSchema) {
    String entityId = generateId();

    return saveEvent(entityType, eventType, eventData, entityId, eventuateSchema);
  }

  public EventInfo saveEvent(String entityType, String eventType, String eventData, String entityId, EventuateSchema eventuateSchema) {
    String eventId = eventuateCommonJdbcOperations.insertIntoEventsTable(idGenerator,
            entityId,
            eventData,
            eventType,
            entityType,
            Optional.empty(),
            Optional.empty(),
            eventuateSchema);

    return new EventInfo(eventData, eventId, entityId);
  }

  public String saveMessage(IdGenerator idGenerator,
                          String payload,
                          String destination,
                          Map<String, String> headers,
                          EventuateSchema eventuateSchema) {
    return eventuateCommonJdbcOperations.insertIntoMessageTable(idGenerator, payload, destination, headers, eventuateSchema);
  }

  public EventInfo updateEvent(String entityId, String eventData) {
    return updateEvent(getTestEntityType(), getTestUpdatedEventType(), entityId, eventData);
  }

  public EventInfo updateEvent(String entityType, String eventType, String entityId, String eventData) {
    String eventId = eventuateCommonJdbcOperations.insertIntoEventsTable(idGenerator,
            entityId,
            eventData,
            eventType,
            entityType,
            Optional.empty(),
            Optional.empty(),
            eventuateSchema);

    return new EventInfo(eventData, eventId, entityId);
  }

  public String generateRandomPayload() {
    return "\"" + "payload-" + generateId() + "\"";
  }

  public String generateId() {
    return StringUtils.rightPad(String.valueOf(System.nanoTime()), String.valueOf(Long.MAX_VALUE).length(), "0");
  }

  public void waitForEventInKafka(KafkaConsumer<String, byte[]> consumer, String entityId, LocalDateTime deadline) {
    while (LocalDateTime.now().isBefore(deadline)) {
      long millis = ChronoUnit.MILLIS.between(LocalDateTime.now(), deadline);
      ConsumerRecords<String, byte[]> records = consumer.poll(millis);
      if (!records.isEmpty()) {
        for (ConsumerRecord<String, byte[]> record : records) {

          Optional<String> entity;
          if (eventuateKafkaMultiMessageConverter.isMultiMessage(record.value())) {
            entity = eventuateKafkaMultiMessageConverter
                    .convertBytesToMessages(record.value())
                    .getMessages()
                    .stream()
                    .map(EventuateKafkaMultiMessage::getKey)
                    .filter(entityId::equals)
                    .findAny();
          }
          else {
            entity = Optional.of(record.key());
          }

          if (entity.isPresent()) {
            return;
          }
        }
      }
    }
    throw new RuntimeException("entity not found: " + entityId);
  }

  public BinlogAssertion<MessageWithDestination> prepareBinlogEntryHandlerMessageAssertion(BinlogEntryReader binlogEntryReader) {
    return prepareBinlogEntryHandlerMessageAssertion(binlogEntryReader, event -> {});
  }

  public BinlogAssertion<MessageWithDestination> prepareBinlogEntryHandlerMessageAssertion(BinlogEntryReader binlogEntryReader,
                                                                                           EventAssertionCallback<MessageWithDestination> onMessageSentCallback) {
    return prepareBinlogEntryHandlerAssertion(binlogEntryReader, new BinlogEntryToMessageConverter(idGenerator), onMessageSentCallback);
  }

  public BinlogAssertion<PublishedEvent> prepareBinlogEntryHandlerEventAssertion(BinlogEntryReader binlogEntryReader) {
    return prepareBinlogEntryHandlerEventAssertion(binlogEntryReader, event -> {});
  }

  public BinlogAssertion<PublishedEvent> prepareBinlogEntryHandlerEventAssertion(BinlogEntryReader binlogEntryReader,
                                                                                 EventAssertionCallback<PublishedEvent> eventAssertionCallback) {
    return prepareBinlogEntryHandlerAssertion(binlogEntryReader, new BinlogEntryToPublishedEventConverter(idGenerator), eventAssertionCallback);
  }

  private <EVENT extends BinLogEvent> BinlogAssertion<EVENT> prepareBinlogEntryHandlerAssertion(BinlogEntryReader binlogEntryReader,
                                                                                                BinlogEntryToEventConverter<EVENT> converter,
                                                                                                EventAssertionCallback<EVENT> eventAssertionCallback) {

    BinlogAssertion<EVENT> binlogAssertion = new BinlogAssertion<>(eventAssertionCallback.waitIterations(), eventAssertionCallback.iterationTimeoutMilliseconds());

    binlogEntryReader.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            converter,
            event -> {
              if (eventAssertionCallback.shouldAddToQueue(event)) {
                binlogAssertion.addEvent(event);
              }

              eventAssertionCallback.onEventSent(event);

              return CompletableFuture.completedFuture(null);
            });

    return binlogAssertion;
  }

  public void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public interface EventAssertionCallback<EVENT extends BinLogEvent> {
    void onEventSent(EVENT event);

    default int waitIterations() {
      return 20;
    }

    default int iterationTimeoutMilliseconds() {
      return 500;
    }

    default boolean shouldAddToQueue(EVENT event) {
      return true;
    }
  }
}
