package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class,
        KafkaOffsetStoreConfiguration.class})
public class MySqlBinaryLogClientTest {

  @Autowired
  private MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private TestHelper testHelper;

  private boolean fail;

  @Test
  public void testRestartOnPublishingException() throws InterruptedException {
    fail = true;

    BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();
    prepareBinlogEntryHandler(publishedEvent -> {
      if (!fail) {
        publishedEvents.add(publishedEvent);
      } else {
        fail = false;
        throw new IllegalStateException("Something happend");
      }
    });

    mySqlBinaryLogClient.start();

    String testCreatedEvent = testHelper.generateTestCreatedEvent();
    TestHelper.EventIdEntityId eventIdEntityId = testHelper.saveEvent(testCreatedEvent);
    testHelper.waitForEvent(publishedEvents, eventIdEntityId.getEventId(), LocalDateTime.now().plusSeconds(60), testCreatedEvent);
    mySqlBinaryLogClient.stop();
  }

  private void prepareBinlogEntryHandler(Consumer<PublishedEvent> consumer) {
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
