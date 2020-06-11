package io.eventuate.local.postgres.wal;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles("PostgresWal")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PostgresWalCdcIntegrationTestConfiguration.class)
public class PostgresWalBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  protected PostgresWalClient postgresWalClient;

  @Override
  protected BinlogEntryReader getBinlogEntryReader() {
    return postgresWalClient;
  }
}
