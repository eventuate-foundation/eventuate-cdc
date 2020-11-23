package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.test.util.EventInfo;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static io.eventuate.local.test.util.assertion.EventAssertOperationBuilder.fromEventInfo;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, KafkaOffsetStoreConfiguration.class})
public class MySqlBinaryLogClientTest {

  @Autowired
  protected MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected BinlogEntryReaderLeadership binlogEntryReaderLeadership;

  private boolean fail;

  @Test
  public void testRestartOnPublishingException() {
    fail = true;

    BinlogAssertion<PublishedEvent> binlogAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(mySqlBinaryLogClient, new TestHelper.EventAssertionCallback<PublishedEvent>() {

      @Override
      public boolean shouldAddToQueue(PublishedEvent event) {
        return !fail;
      }

      @Override
      public void onEventSent(PublishedEvent event) {
        if (fail) {
          fail = false;
          throw new IllegalStateException("Something happened");
        }
      }

      @Override
      public int waitIterations() {
        return 120;
      }
    });

    binlogEntryReaderLeadership.start();

    EventInfo eventInfo = testHelper.saveRandomEvent();

    binlogAssertion.assertEventReceived(fromEventInfo(eventInfo).build());

    binlogEntryReaderLeadership.stop();
  }
}
