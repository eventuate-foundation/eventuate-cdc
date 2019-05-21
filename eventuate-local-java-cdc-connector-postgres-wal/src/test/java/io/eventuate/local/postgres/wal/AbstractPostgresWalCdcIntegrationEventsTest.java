package io.eventuate.local.postgres.wal;

import io.eventuate.common.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.AbstractCdcEventsTest;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class AbstractPostgresWalCdcIntegrationEventsTest extends AbstractCdcEventsTest {

  @Value("${spring.datasource.url}")
  private String dataSourceURL;

  @Value("${spring.datasource.username}")
  private String dbUserName;

  @Value("${spring.datasource.password}")
  private String dbPassword;

  @Autowired
  private PostgresWalClient postgresWalClient;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Test
  public void shouldGetEvents() throws InterruptedException{

    BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();

    postgresWalClient.addBinlogEntryHandler(
            eventuateSchema,
            sourceTableNameSupplier.getSourceTableName(),
            new BinlogEntryToPublishedEventConverter(),
            new CdcDataPublisher<PublishedEvent>(null, null, null, null) {
              @Override
              public void handleEvent(PublishedEvent publishedEvent) throws EventuateLocalPublishingException {
                publishedEvents.add(publishedEvent);
              }
            });

    postgresWalClient.start();

    String testCreatedEvent = generateTestCreatedEvent();
    EventIdEntityId saveResult = saveEvent(testCreatedEvent);

    String testUpdatedEvent = generateTestUpdatedEvent();
    EventIdEntityId updateResult = updateEvent(saveResult.getEntityId(), testUpdatedEvent);

    LocalDateTime deadline = LocalDateTime.now().plusSeconds(20);

    waitForEvent(publishedEvents, saveResult.getEventId(), deadline, testCreatedEvent);
    waitForEvent(publishedEvents, updateResult.getEventId(), deadline, testUpdatedEvent);
    postgresWalClient.stop();
  }

}
