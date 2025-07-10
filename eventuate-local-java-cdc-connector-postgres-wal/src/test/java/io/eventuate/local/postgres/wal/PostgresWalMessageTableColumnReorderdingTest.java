package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class)
public class PostgresWalMessageTableColumnReorderdingTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterColumnReordering() {
    super.testNewMessageHandledAfterColumnReordering();
  }
}
