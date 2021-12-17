package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class)
public class PostgresWalMessageTableColumnReorderdingTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterColumnReordering() {
    super.testNewMessageHandledAfterColumnReordering();
  }
}
