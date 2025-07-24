package io.eventuate.local.polling;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.OutboxPartitioningSpec;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.EventuateSchemaConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.TestHelperConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
  @Import({EventuateSchemaConfiguration.class, SqlDialectConfiguration.class, IdGeneratorConfiguration.class, TestHelperConfiguration.class})
  public static class Config {
    @Bean
    public EventuateConfigurationProperties eventuateConfigurationProperties() {
      return new EventuateConfigurationProperties();
    }


  }

  protected static final int EVENTS_PER_POLLING_ITERATION = 3;
  protected static final int NUMBER_OF_EVENTS_TO_PUBLISH = 10;

  @Autowired
  protected IdGenerator idGenerator;

  @Value("${spring.datasource.url}")
  private String dataSourceURL;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  @Autowired
  protected EventuateSchema eventuateSchema;


  @Autowired
  private DataSource dataSource;

  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected EventuateConfigurationProperties eventuateConfigurationProperties;

  protected AtomicInteger processedEvents;

  protected PollingDao pollingDao;

  @BeforeEach
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
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
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
            new ParallelPollingChannels(new HashSet<>(Arrays.asList(eventuateConfigurationProperties.getPollingParallelChannels()))),
            new OutboxPartitioningSpec(eventuateConfigurationProperties.getOutboxTables(), eventuateConfigurationProperties.getOutboxTablePartitions()));
  }

  protected void assertEventsArePublished(List<String> eventIds) {
    eventIds.forEach(this::assertEventIsPublished);
  }

  private void assertEventIsPublished(String id) {
    Map<String, Object> event = jdbcTemplate.queryForMap("select * from %s where event_id = ?".formatted(eventuateSchema.qualifyTable("events")), id);
    Assertions.assertEquals(1, ((Number)event.get("published")).intValue());
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
    jdbcTemplate.execute("update %s set published = 1".formatted(eventuateSchema.qualifyTable("events")));
  }


}
