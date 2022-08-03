package io.eventuate.local.polling;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateSchemaConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.test.util.TestHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class AbstractPollingDaoIntegrationTest {


  @Configuration
  @Import({EventuateSchemaConfiguration.class, SqlDialectConfiguration.class, IdGeneratorConfiguration.class})
  public static class Config {
    @Bean
    public EventuateConfigurationProperties eventuateConfigurationProperties() {
      return new EventuateConfigurationProperties();
    }

    @Bean
    public TestHelper testHelper() {
      return new TestHelper();
    }


  }

  protected static final int EVENTS_PER_POLLING_ITERATION = 3;
  protected static final int NUMBER_OF_EVENTS_TO_PUBLISH = 10;

  @Autowired
  private IdGenerator idGenerator;

  @Value("${spring.datasource.url}")
  private String dataSourceURL;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  @Autowired
  private EventuateSchema eventuateSchema;


  @Autowired
  private DataSource dataSource;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  protected EventuateConfigurationProperties eventuateConfigurationProperties;

  protected AtomicInteger processedEvents;

  protected PollingDao pollingDao;

  @Before
  public void init() {
    processedEvents = new AtomicInteger(0);
    pollingDao = createPollingDao();
    markAllEventsAsPublished();
  }


  protected List<String> saveEvents() {
    List<String> eventIds = new ArrayList<>();

    for (int i = 0; i < NUMBER_OF_EVENTS_TO_PUBLISH; i++) {
      eventIds.add(testHelper.saveRandomEvent().getEventId());
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
            eventuateConfigurationProperties.getOutboxId(),
            new ParallelPollingChannels(new HashSet<>(Arrays.asList(eventuateConfigurationProperties.getPollingParallelChannels()))));
  }

  protected void assertEventsArePublished(List<String> eventIds) {
    eventIds.forEach(this::assertEventIsPublished);
  }

  private void assertEventIsPublished(String id) {
    Map<String, Object> event = jdbcTemplate.queryForMap(String.format("select * from %s where event_id = ?", eventuateSchema.qualifyTable("events")), id);
    Assert.assertEquals(1, ((Number)event.get("published")).intValue());
  }

  protected BinlogEntryHandler prepareBinlogEntryHandler(CompletableFuture<?> resultWhenEventConsumed) {
    return pollingDao.addBinlogEntryHandler(eventuateSchema,
            "events",
            new BinlogEntryToPublishedEventConverter(idGenerator),
            event -> {
              processedEvents.incrementAndGet();
              return resultWhenEventConsumed;
            });
  }

  private void markAllEventsAsPublished() {
    jdbcTemplate.execute(String.format("update %s set published = 1", eventuateSchema.qualifyTable("events")));
  }


}
