package io.eventuate.local.polling;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.AbstractCdcEventsTest;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@ActiveProfiles("EventuatePolling")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class, properties= {"eventuatelocal.cdc.max.events.per.polling=3"})
@EnableAutoConfiguration
public class PollingDaoIntegrationTest extends AbstractCdcEventsTest {

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private PollingDao pollingDao;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  private int processedEvents;

  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  @Before
  public void init() {
    super.init();
    processedEvents = 0;
  }

  @Test
  public void testThatPollingEventCountAreLimited() {
    final int EVENTS = 10;
    final int EVENTS_PER_POLLING_ITERATION = 3;

    clearEventsTable();

    prepareBinlogEntryHandler(publishedEvent -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      processedEvents++;
    });

    List<String> eventIds = new ArrayList<>();

    for (int i = 0; i < EVENTS; i++) {
      eventIds.add(saveEvent(generateTestCreatedEvent()).getEventId());
    }

    startEventProcessing();

    for (int i = 1; i <= EVENTS / EVENTS_PER_POLLING_ITERATION; i++) {
      assertEventsAreProcessed(EVENTS_PER_POLLING_ITERATION * i, i);
    }

    assertEventsAreProcessed(EVENTS, EVENTS / EVENTS_PER_POLLING_ITERATION + EVENTS % EVENTS_PER_POLLING_ITERATION);
    assertEventsArePublished(eventIds);

    stopEventProcessing();
  }

  private void assertEventsAreProcessed(int events, int iteration) {
    Eventually.eventually(1000, 10, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(events, processedEvents);
      Assert.assertEquals(iteration, cdcDataPublisher.getPollingIterations());
    });
  }

  private void assertEventsArePublished(List<String> eventIds) {
    eventIds.forEach(this::assertEventIsPublished);
  }

  private void assertEventIsPublished(String id) {
    Map<String, Object> event = jdbcTemplate.queryForMap(String.format("select * from %s where event_id = ?", eventuateSchema.qualifyTable("events")), id);
    Assert.assertEquals(1, ((Number)event.get("published")).intValue());
  }

  protected void prepareBinlogEntryHandler(Consumer<PublishedEvent> consumer) {
    cdcDataPublisher = new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
      @Override
      public void handleEvent(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
        consumer.accept(publishedEvent);
      }
    };

    pollingDao.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            cdcDataPublisher);
  }

  private void startEventProcessing() {
    pollingDao.start();
  }

  private void stopEventProcessing() {
    pollingDao.stop();
  }

  private void clearEventsTable() {
    jdbcTemplate.execute(String.format("update %s set published = 1", eventuateSchema.qualifyTable("events")));
  }
}
