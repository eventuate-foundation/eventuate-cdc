package io.eventuate.local.mysql.binlog;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class, OffsetStoreMockConfiguration.class})
public class MySqlBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  protected MySqlBinaryLogClient mySqlBinaryLogClient;

  @Override
  protected BinlogEntryReader getBinlogEntryReader() {
    return mySqlBinaryLogClient;
  }
}
