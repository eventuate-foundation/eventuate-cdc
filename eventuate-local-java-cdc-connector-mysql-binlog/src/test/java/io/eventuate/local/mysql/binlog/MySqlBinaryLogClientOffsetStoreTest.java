package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryToPublishedEventConverter;
import io.eventuate.local.common.CdcDataPublisher;
import io.eventuate.local.common.exception.EventuateLocalPublishingException;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, MySqlBinaryLogClientOffsetStoreTest.OffsetStoreConfiguration.class})
public class MySqlBinaryLogClientOffsetStoreTest {

  public static class OffsetStoreConfiguration {
    @Bean
    @Primary
    public OffsetStore offsetStore() {
        return new OffsetStore() {
          private boolean throwExceptionOnSave = true;
          private boolean throwExceptionOnLoad = true;
          private Optional<BinlogFileOffset> offset = Optional.empty();

          @Override
          public Optional<BinlogFileOffset> getLastBinlogFileOffset() {
            if (throwExceptionOnLoad) {
              throwExceptionOnLoad = false;
//              throw new IllegalStateException("Something happened");
            }

            return offset;
          }

          @Override
          public void save(BinlogFileOffset binlogFileOffset) {
            if (throwExceptionOnSave) {
              throwExceptionOnSave = false;
              throw new IllegalStateException("Something happened");
            }
            offset = Optional.of(binlogFileOffset);
          }
        };
      }
  }

  @Autowired
  private MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  private SourceTableNameSupplier sourceTableNameSupplier;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private TestHelper testHelper;

  @Test
  public void testRestartOnException() throws InterruptedException {
    BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();
    prepareBinlogEntryHandler(publishedEvents::add);

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
