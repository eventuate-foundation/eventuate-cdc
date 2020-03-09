package io.eventuate.local.test.util;

import io.eventuate.common.common.spring.jdbc.EventuateSpringJdbcStatementExecutor;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestHelper {
  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private EventuateSchema eventuateSchema;

  private EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

  @PostConstruct
  public void init() {
    eventuateCommonJdbcOperations = new EventuateCommonJdbcOperations(new EventuateSpringJdbcStatementExecutor(jdbcTemplate));
  }

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

  public String generateTestUpdatedEvent() {
    return generateId();
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

  public EventIdEntityId saveEvent(String eventData) {
    return saveEvent(getTestEntityType(), getTestCreatedEventType(), eventData, eventuateSchema);
  }

  public EventIdEntityId saveEvent(String entityType, String eventType, String eventData, EventuateSchema eventuateSchema) {
    String eventId = generateId();
    String entityId = generateId();

    return saveEvent(entityType, eventType, eventData, eventId, entityId, eventuateSchema);
  }

  public EventIdEntityId saveEvent(String entityType, String eventType, String eventData, String eventId, String entityId, EventuateSchema eventuateSchema) {
    eventuateCommonJdbcOperations.insertIntoEventsTable(eventId,
            entityId,
            eventData,
            eventType,
            entityType,
            Optional.empty(),
            Optional.empty(),
            eventuateSchema);

    return new EventIdEntityId(eventId, entityId);
  }

  public EventIdEntityId updateEvent(String entityId, String eventData) {
    return updateEvent(getTestEntityType(), getTestUpdatedEventType(), entityId, eventData);
  }

  public EventIdEntityId updateEvent(String entityType, String eventType, String entityId, String eventData) {
    String eventId = generateId();

    eventuateCommonJdbcOperations.insertIntoEventsTable(eventId,
            entityId,
            eventData,
            eventType,
            entityType,
            Optional.empty(),
            Optional.empty(),
            eventuateSchema);

    return new EventIdEntityId(eventId, entityId);
  }

  public String generateTestCreatedEvent() {
    return generateId();
  }

  public String generateId() {
    return StringUtils.rightPad(String.valueOf(System.nanoTime()), String.valueOf(Long.MAX_VALUE).length(), "0");
  }

  public void waitForEvent(BlockingQueue<PublishedEvent> publishedEvents, String eventId, LocalDateTime deadline, String eventData) throws InterruptedException {
    while (LocalDateTime.now().isBefore(deadline)) {
      long millis = ChronoUnit.MILLIS.between(deadline, LocalDateTime.now());
      PublishedEvent event = publishedEvents.poll(millis, TimeUnit.MILLISECONDS);
      if (event != null && event.getId().equals(eventId) && eventData.equals(event.getEventData()))
        return;
    }
    throw new RuntimeException("event not found: " + eventId);
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

  public void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static class EventIdEntityId {
    private String eventId;
    private String entityId;

    public EventIdEntityId(String eventId, String entityId) {
      this.eventId = eventId;
      this.entityId = entityId;
    }

    public String getEventId() {
      return eventId;
    }

    public void setEventId(String eventId) {
      this.eventId = eventId;
    }

    public String getEntityId() {
      return entityId;
    }

    public void setEntityId(String entityId) {
      this.entityId = entityId;
    }
  }
}
