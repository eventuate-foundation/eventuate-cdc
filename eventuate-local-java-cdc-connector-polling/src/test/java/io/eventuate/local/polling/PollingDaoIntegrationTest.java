package io.eventuate.local.polling;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@ActiveProfiles("EventuatePolling")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
@EnableAutoConfiguration
public class PollingDaoIntegrationTest {

  private static final int EVENTS_PER_POLLING_ITERATION = 3;

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

  private int processedEvents;

  private CdcDataPublisher<PublishedEvent> cdcDataPublisher;

  private PollingDao pollingDao;

  @Before
  public void init() {
    processedEvents = 0;
    pollingDao = createPollingDao();
  }

  @Test
  public void testThatPollingEventCountAreLimited() {
    final int EVENTS = 10;

    clearEventsTable();

    BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(publishedEvent -> {
      processedEvents++;
    });

    List<String> eventIds = new ArrayList<>();

    for (int i = 0; i < EVENTS; i++) {
      eventIds.add(testHelper.saveEvent(testHelper.generateTestCreatedEvent()).getEventId());
    }

    pollingDao.processEvents(binlogEntryHandler);

    Assert.assertEquals(EVENTS_PER_POLLING_ITERATION, processedEvents);

    assertEventsArePublished(eventIds.subList(0, EVENTS_PER_POLLING_ITERATION));
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
            sqlDialectSelector.getDialect(driver));
  }

  private void assertEventsArePublished(List<String> eventIds) {
    eventIds.forEach(this::assertEventIsPublished);
  }

  private void assertEventIsPublished(String id) {
    Map<String, Object> event = jdbcTemplate.queryForMap(String.format("select * from %s where event_id = ?", eventuateSchema.qualifyTable("events")), id);
    Assert.assertEquals(1, ((Number)event.get("published")).intValue());
  }

  protected BinlogEntryHandler prepareBinlogEntryHandler(Consumer<PublishedEvent> consumer) {
    cdcDataPublisher = new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
      @Override
      public Optional<CompletableFuture<?>> handleEvent(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
        consumer.accept(publishedEvent);
        return Optional.empty();
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
