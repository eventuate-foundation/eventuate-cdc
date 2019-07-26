package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
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

public abstract class AbstractMySqlBinlogCdcIntegrationEventsTest {

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

  @Autowired
  private TestHelper testHelper;

  private String dataFile = "../scripts/initialize-mysql.sql";

  @Value("${spring.datasource.driver.class.name}")
  private String driverClassName;

  @Test
  public void shouldGetEvents() throws InterruptedException {
    try {
      BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();

      prepareBinlogEntryHandler(publishedEvents::add);
      mySqlBinaryLogClient.start();

      String testCreatedEvent = testHelper.generateTestCreatedEvent();
      TestHelper.EventIdEntityId saveResult = testHelper.saveEvent(testCreatedEvent);

      String testUpdatedEvent = testHelper.generateTestUpdatedEvent();
      TestHelper.EventIdEntityId updateResult = testHelper.updateEvent(saveResult.getEntityId(), testUpdatedEvent);

      // Wait for 10 seconds
      LocalDateTime deadline = LocalDateTime.now().plusSeconds(40);

      testHelper.waitForEvent(publishedEvents, saveResult.getEventId(), deadline, testCreatedEvent);
      testHelper.waitForEvent(publishedEvents, updateResult.getEventId(), deadline, testUpdatedEvent);
    } finally {
      mySqlBinaryLogClient.stop();
    }
  }

  @Test
  public void shouldGetEventsFromOnlyEventuateSchema() throws InterruptedException {

    String otherSchemaName = "custom" + System.currentTimeMillis();

    createOtherSchema(otherSchemaName);

    TestHelper.EventIdEntityId otherSaveResult = insertEventIntoOtherSchema(otherSchemaName);

    try {
      BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();

      prepareBinlogEntryHandler(publishedEvents::add);
      mySqlBinaryLogClient.start();

      String testCreatedEvent = testHelper.generateTestCreatedEvent();
      TestHelper.EventIdEntityId saveResult = testHelper.saveEvent(testCreatedEvent);

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

  private TestHelper.EventIdEntityId insertEventIntoOtherSchema(String otherSchemaName) {
    return testHelper.saveEvent("Other", testHelper.getTestCreatedEventType(), testHelper.generateTestCreatedEvent(), new EventuateSchema(otherSchemaName));
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
