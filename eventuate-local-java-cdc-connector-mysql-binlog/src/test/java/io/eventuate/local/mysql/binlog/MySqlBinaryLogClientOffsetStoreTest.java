package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class,
        MySqlBinaryLogClientOffsetStoreTest.OffsetStoreConfiguration.class,
        SqlDialectConfiguration.class})
public class MySqlBinaryLogClientOffsetStoreTest extends AbstractMySqlBinaryLogClientTest {

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
              throw new IllegalStateException("Something happened");
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

  @Test
  public void testRestartOnException() throws InterruptedException {
    BlockingQueue<PublishedEvent> publishedEvents = new LinkedBlockingDeque<>();
    prepareBinlogEntryHandler(publishedEvents::add);

    binlogEntryReaderLeadership.start();

    String testCreatedEvent = testHelper.generateTestCreatedEvent();
    TestHelper.EventIdEntityId eventIdEntityId = testHelper.saveEvent(testCreatedEvent);
    testHelper.waitForEvent(publishedEvents, eventIdEntityId.getEventId(), LocalDateTime.now().plusSeconds(60), testCreatedEvent);
    binlogEntryReaderLeadership.stop();
  }
}
