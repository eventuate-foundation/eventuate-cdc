package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.EventInfo;
import io.eventuate.local.test.util.TestHelper;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;

import static io.eventuate.local.test.util.assertion.EventAssertOperationBuilder.fromEventInfo;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class,
        MySqlBinaryLogClientOffsetStoreTest.OffsetStoreConfiguration.class})
public class MySqlBinaryLogClientOffsetStoreTest {

  @Autowired
  protected MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected BinlogEntryReaderLeadership binlogEntryReaderLeadership;

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
  public void testRestartOnException() {
    BinlogAssertion<PublishedEvent> binlogAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(mySqlBinaryLogClient);

    binlogEntryReaderLeadership.start();

    EventInfo eventInfo = testHelper.saveRandomEvent();

    binlogAssertion.assertEventReceived(fromEventInfo(eventInfo).build());

    binlogEntryReaderLeadership.stop();
  }
}
