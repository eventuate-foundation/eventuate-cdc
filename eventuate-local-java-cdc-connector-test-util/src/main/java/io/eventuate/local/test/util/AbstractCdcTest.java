package io.eventuate.local.test.util;

import io.eventuate.Int128;
import io.eventuate.common.PublishedEvent;
import io.eventuate.example.banking.domain.Account;
import io.eventuate.example.banking.domain.AccountCreatedEvent;
import io.eventuate.example.banking.domain.AccountDebitedEvent;
import io.eventuate.javaclient.commonimpl.*;
import io.eventuate.javaclient.spring.jdbc.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AbstractCdcTest extends AbstractConnectorTest{
  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  private EventuateSchema eventuateSchema;

  private IdGenerator idGenerator = new IdGeneratorImpl();

  public String generateAccountCreatedEvent() {
    return JSonMapper.toJson(new AccountCreatedEvent(new BigDecimal(System.currentTimeMillis())));
  }

  public String generateAccountDebitedEvent() {
    return JSonMapper.toJson(new AccountDebitedEvent(new BigDecimal(System.currentTimeMillis()), null));
  }

  public String getEventTopicName() {
    return Account.class.getTypeName();
  }

  public EntityIdVersionAndEventIds saveEvent(String eventData) {
    return saveEvent(eventData, Account.class.getTypeName(), eventuateSchema);
  }

  public EntityIdVersionAndEventIds saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema) {
    Int128 eventId = idGenerator.genId();
    String entityId = idGenerator.genId().asString();

    jdbcTemplate.update(String.format("INSERT INTO %s (event_id, event_type, event_data, entity_type, entity_id, triggering_event, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)", eventuateSchema.qualifyTable("events")),
            eventId.asString(), eventType, eventData, eventType, entityId,
            null,
            null);

    return new EntityIdVersionAndEventIds(entityId, eventId, Collections.singletonList(eventId));
  }

  public EntityIdVersionAndEventIds updateEvent(String entityId, Int128 entityVersion, String eventData) {
    EventTypeAndData event = new EventTypeAndData(AccountCreatedEvent.class.getTypeName(), eventData, Optional.empty());

    EventIdTypeAndData eventsWithId = toEventWithId(event);

    String entityType = Account.class.getTypeName();

    jdbcTemplate.update(String.format("INSERT INTO %s (event_id, event_type, event_data, entity_type, entity_id, triggering_event, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)", eventuateSchema.qualifyTable("events")),
            eventsWithId.getId().asString(),
            event.getEventType(),
            eventData,
            entityType,
            entityId,
            null,
            event.getMetadata().orElse(null));


    return new EntityIdVersionAndEventIds(entityId,
            eventsWithId.getId(),
            Collections.singletonList(eventsWithId.getId()));
  }

  public PublishedEvent waitForEvent(BlockingQueue<PublishedEvent> publishedEvents, Int128 eventId, LocalDateTime deadline, String eventData) throws InterruptedException {
    while (LocalDateTime.now().isBefore(deadline)) {
      long millis = ChronoUnit.MILLIS.between(deadline, LocalDateTime.now());
      PublishedEvent event = publishedEvents.poll(millis, TimeUnit.MILLISECONDS);
      if (event != null && event.getId().equals(eventId.asString()) && eventData.equals(event.getEventData()))
        return event;
    }
    throw new RuntimeException("event not found: " + eventId);
  }

  public void waitForEventInKafka(KafkaConsumer<String, String> consumer, String entityId, LocalDateTime deadline) {
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

  private EventIdTypeAndData toEventWithId(EventTypeAndData eventTypeAndData) {
    return new EventIdTypeAndData(idGenerator.genId(), eventTypeAndData.getEventType(), eventTypeAndData.getEventData(), eventTypeAndData.getMetadata());
  }
}
