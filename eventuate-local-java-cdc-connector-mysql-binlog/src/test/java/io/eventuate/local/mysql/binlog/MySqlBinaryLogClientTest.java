package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.test.util.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class,
        KafkaOffsetStoreConfiguration.class})
public class MySqlBinaryLogClientTest extends AbstractMySqlBinaryLogClientTest {

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

    binlogEntryReaderLeadership.start();

    String testCreatedEvent = testHelper.generateTestCreatedEvent();
    TestHelper.EventIdEntityId eventIdEntityId = testHelper.saveEvent(testCreatedEvent);
    testHelper.waitForEvent(publishedEvents, eventIdEntityId.getEventId(), LocalDateTime.now().plusSeconds(60), testCreatedEvent);
    binlogEntryReaderLeadership.stop();
  }
}
