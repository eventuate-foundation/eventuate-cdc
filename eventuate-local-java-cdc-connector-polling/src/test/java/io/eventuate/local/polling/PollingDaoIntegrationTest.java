package io.eventuate.local.polling;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@ActiveProfiles("${SPRING_PROFILES_ACTIVE:EventuatePolling}")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
@EnableAutoConfiguration
public class PollingDaoIntegrationTest {

  private static final int EVENTS_PER_POLLING_ITERATION = 3;
  private static final int EVENTS = 10;


  @Value("${spring.datasource.url}")
  private String dataSourceURL;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private DataSource dataSource;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private EventuateConfigurationProperties eventuateConfigurationProperties;

  private AtomicInteger processedEvents;

  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  private PollingDao pollingDao;

  @Before
  public void init() {
    processedEvents = new AtomicInteger(0);
    pollingDao = createPollingDao();
    clearEventsTable();
  }

  @Test
  public void testThatPollingEventCountAreLimited() {
    BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(CompletableFuture.completedFuture(null));

    List<String> eventIds = saveEvents();

    pollingDao.processEvents(binlogEntryHandler);

    Assert.assertEquals(EVENTS_PER_POLLING_ITERATION, processedEvents.get());

    assertEventsArePublished(eventIds.subList(0, EVENTS_PER_POLLING_ITERATION));
  }

  @Test
  public void testMessagesAreNotProcessedTwice() throws InterruptedException {
    CompletableFuture<?> completableFuture = new CompletableFuture();

    BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(completableFuture);

    saveEvents();

    CountDownLatch allIterationsComplete = new CountDownLatch(1);

    CompletableFuture.supplyAsync(() -> {
      for (int i = 0; i < (EVENTS / EVENTS_PER_POLLING_ITERATION) * 2; i++) {
        pollingDao.processEvents(binlogEntryHandler);
      }
      allIterationsComplete.countDown();
      return null;
    });

    Thread.sleep(3000);
    completableFuture.complete(null);
    allIterationsComplete.await();

    Assert.assertEquals(EVENTS, processedEvents.get());
  }

  private List<String> saveEvents() {
    List<String> eventIds = new ArrayList<>();

    for (int i = 0; i < EVENTS; i++) {
      eventIds.add(testHelper.saveEvent(testHelper.generateTestCreatedEvent()).getEventId());
    }

    return eventIds;
  }

  private PollingDao createPollingDao() {
    MeterRegistry meterRegistry = Mockito.mock(MeterRegistry.class);
    Mockito.when(meterRegistry.counter(Mockito.anyString(), Mockito.anyCollection())).thenReturn(Mockito.mock(Counter.class));

    return new PollingDao(meterRegistry,
            dataSourceURL,
            dataSource,
            EVENTS_PER_POLLING_ITERATION,
            10,
            100,
            1000,
            testHelper.generateId(),
            sqlDialectSelector.getDialect(driver),
            eventuateConfigurationProperties.getReaderId());
  }

  private void assertEventsArePublished(List<String> eventIds) {
    eventIds.forEach(this::assertEventIsPublished);
  }

  private void assertEventIsPublished(String id) {
    Map<String, Object> event = jdbcTemplate.queryForMap(String.format("select * from %s where event_id = ?", eventuateSchema.qualifyTable("events")), id);
    Assert.assertEquals(1, ((Number)event.get("published")).intValue());
  }

  private BinlogEntryHandler prepareBinlogEntryHandler(CompletableFuture<?> resultWhenEventConsumed) {
    cdcDataPublisher = new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
      @Override
      public CompletableFuture<?> sendMessage(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
        processedEvents.incrementAndGet();
        return resultWhenEventConsumed;
      }
    };

    return pollingDao.addBinlogEntryHandler(eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            cdcDataPublisher);
  }

  private void clearEventsTable() {
    jdbcTemplate.execute(String.format("update %s set published = 1", eventuateSchema.qualifyTable("events")));
  }
}
