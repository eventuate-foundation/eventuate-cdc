package io.eventuate.cdc.e2e.common;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractEventuateCdcTest {

  protected String subscriberId = generateId();

  @Autowired
  protected EventuateCommonJdbcOperations eventuateCommonJdbcOperations;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  @Autowired
  protected IdGenerator idGenerator;


  @Test
  public void insertToEventTableAndWaitEventInBroker() throws Exception {
    String destination = generateId();

    String data = generateId() + getClass().getName();
    String rawData = "\"" + data + "\"";

    ConcurrentLinkedQueue<String> messageQueue = new ConcurrentLinkedQueue<>();

    createConsumer(destination, messageQueue::add);
    saveEvent(rawData, destination, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA), true);
    String eventId = saveEvent(rawData, destination, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA), false);

    Eventually.eventually(120, 500, TimeUnit.MILLISECONDS, () -> assertTrue(messageQueue.size() > 0));

    Assertions.assertEquals(1, messageQueue.size());

    String m = messageQueue.poll();

    Map<String, Object> message = JSonMapper.fromJson(m, Map.class);

    assertEquals(eventId, extractEventId(message));
    assertEquals(rawData, extractEventPayload(message));
  }

  protected String generateId() {
    return UUID.randomUUID().toString();
  }

  protected abstract void createConsumer(String topic, Consumer<String> consumer) throws Exception;
  protected abstract String saveEvent(String eventData, String eventType, EventuateSchema eventuateSchema, boolean published);

  protected abstract String extractEventId(Map<String, Object> eventAsMap);
  protected abstract String extractEventPayload(Map<String, Object> eventAsMap);
}
