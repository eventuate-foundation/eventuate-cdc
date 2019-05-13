package io.eventuate.local.mysql.binlog;

import io.eventuate.common.PublishedEvent;
import io.eventuate.javaclient.spring.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.AbstractCdcEventsTest;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.testutil.CustomDBCreator;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractMySqlBinlogCdcIntegrationEventsTest extends AbstractCdcEventsTest {

  @Value("${spring.datasource.url}")
  private String dataSourceUrl;

  @Autowired
  private EventuateConfigurationProperties eventuateConfigurationProperties;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private MySqlBinaryLogClient mySqlBinaryLogClient;

  private String dataFile = "../mysql/1.initialize-database.sql";

  @Value("${spring.datasource.driver.class.name}")
  private String driverClassName;

  @Test
  public void shouldGetEvents() throws InterruptedException {
    try {
      BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();

      prepareBinlogEntryHandler(publishedEvents::add);
      mySqlBinaryLogClient.start();

      String testCreatedEvent = generateTestCreatedEvent();
      EventIdEntityId saveResult = saveEvent(testCreatedEvent);

      String testUpdatedEvent = generateTestUpdatedEvent();
      EventIdEntityId updateResult = updateEvent(saveResult.getEntityId(), testUpdatedEvent);

      // Wait for 10 seconds
      LocalDateTime deadline = LocalDateTime.now().plusSeconds(40);

      waitForEvent(publishedEvents, saveResult.getEventId(), deadline, testCreatedEvent);
      waitForEvent(publishedEvents, updateResult.getEventId(), deadline, testUpdatedEvent);
    } finally {
      mySqlBinaryLogClient.stop();
    }
  }

  @Test
  public void shouldGetEventsFromOnlyEventuateSchema() throws InterruptedException {

    String otherSchemaName = "custom" + System.currentTimeMillis();

    createOtherSchema(otherSchemaName);

    EventIdEntityId otherSaveResult = insertEventIntoOtherSchema(otherSchemaName);

    try {
      BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();

      prepareBinlogEntryHandler(publishedEvents::add);
      mySqlBinaryLogClient.start();

      String testCreatedEvent = generateTestCreatedEvent();
      EventIdEntityId saveResult = saveEvent(testCreatedEvent);

      LocalDateTime deadline = LocalDateTime.now().plusSeconds(40);

      String eventId = saveResult.getEventId();
      String eventData = testCreatedEvent;

      boolean foundEvent = false;

      while (!foundEvent && LocalDateTime.now().isBefore(deadline)) {
        long millis = ChronoUnit.MILLIS.between(deadline, LocalDateTime.now());
        PublishedEvent event = publishedEvents.poll(millis, TimeUnit.MILLISECONDS);
        if (event != null) {
          System.out.println("Got: " + event);
          if (event.getId().equals(eventId) && eventData.equals(event.getEventData())) {
              foundEvent = true;
          } else if (event.getId().equals(otherSaveResult.getEventId())) {
              fail("Found event inserted into other schema");
          }
        }
      }
      if (!foundEvent)
        throw new RuntimeException("event not found: " + eventId);

    } finally {
      mySqlBinaryLogClient.stop();
    }
  }

  private EventIdEntityId insertEventIntoOtherSchema(String otherSchemaName) {
    return saveEvent("Other", getTestCreatedEventType(), generateTestCreatedEvent(), new EventuateSchema(otherSchemaName));
  }

  private void createOtherSchema(String otherSchemaName) {
    CustomDBCreator dbCreator = new CustomDBCreator(dataFile, dataSourceUrl, driverClassName, eventuateConfigurationProperties.getDbUserName(), eventuateConfigurationProperties.getDbPassword());
    dbCreator.create(sqlList -> {
      sqlList.set(0, sqlList.get(0).replace("create database", "create database if not exists"));
      for (int i = 0; i < 3; i++) sqlList.set(i, sqlList.get(i).replace("eventuate", otherSchemaName));
      return sqlList;
    });
  }

  protected void prepareBinlogEntryHandler(Consumer<PublishedEvent> consumer) {
    mySqlBinaryLogClient.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
              @Override
              public void handleEvent(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
                consumer.accept(publishedEvent);
              }
            });
  }
}
