package io.eventuate.local.polling;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("${SPRING_PROFILES_ACTIVE:EventuatePolling}")
@SpringBootTest(classes = PollingBinlogEntryReaderMessageTableTestConfiguration.class)
public class PollingMessageTableColumnReorderdingTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterColumnReordering() {
    super.testNewMessageHandledAfterColumnReordering();
  }
}
