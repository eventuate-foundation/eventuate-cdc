package io.eventuate.local.test.util;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AbstractCdcEventsTest extends AbstractConnectorTest {
  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  private EventuateSchema eventuateSchema;

  private EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  @Before
  public void init() {
    eventuateCommonJdbcOperations = new EventuateCommonJdbcOperations(jdbcTemplate);
  }

  protected String generateTestCreatedEvent() {
    return generateId();
  }

  protected String generateTestUpdatedEvent() {
    return generateId();
  }

  protected String getEventTopicName() {
    return "TestEntity";
  }

  protected String getTestEntityType() {
    return "TestEntity";
  }

  protected String getTestCreatedEventType() {
    return "TestCreatedEvent";
  }

  protected String getTestUpdatedEventType() {
    return "TestUpdatedEvent";
  }

  protected EventIdEntityId saveEvent(String eventData) {
    return saveEvent(getTestEntityType(), getTestCreatedEventType(), eventData, eventuateSchema);
  }

  protected EventIdEntityId saveEvent(String entityType, String eventType, String eventData, EventuateSchema eventuateSchema) {
    String eventId = generateId();
    String entityId = generateId();

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

  protected EventIdEntityId updateEvent(String entityId, String eventData) {
    return updateEvent(getTestEntityType(), getTestUpdatedEventType(), entityId, eventData);
  }

  protected EventIdEntityId updateEvent(String entityType, String eventType, String entityId, String eventData) {
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

  protected void waitForEvent(BlockingQueue<PublishedEvent> publishedEvents, String eventId, LocalDateTime deadline, String eventData) throws InterruptedException {
    while (LocalDateTime.now().isBefore(deadline)) {
      long millis = ChronoUnit.MILLIS.between(deadline, LocalDateTime.now());
      PublishedEvent event = publishedEvents.poll(millis, TimeUnit.MILLISECONDS);
      if (event != null && event.getId().equals(eventId) && eventData.equals(event.getEventData()))
        return;
    }
    throw new RuntimeException("event not found: " + eventId);
  }

  protected void waitForEventInKafka(KafkaConsumer<String, String> consumer, String entityId, LocalDateTime deadline) {
    while (LocalDateTime.now().isBefore(deadline)) {
      long millis = ChronoUnit.MILLIS.between(LocalDateTime.now(), deadline);
      ConsumerRecords<String, String> records = consumer.poll(millis);
      if (!records.isEmpty()) {
        for (ConsumerRecord<String, String> record : records) {
          if (record.key().equals(entityId)) {
            return;
          }
        }
      }
    }
    throw new RuntimeException("entity not found: " + entityId);
  }

  protected String generateId() {
    return UUID.randomUUID().toString();
  }

  protected class EventIdEntityId {
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
