package io.eventuate.local.polling;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles("EventuatePolling")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  protected PollingDao pollingDao;

  @Override
  protected BinlogEntryReader getBinlogEntryReader() {
    return pollingDao;
  }
}
