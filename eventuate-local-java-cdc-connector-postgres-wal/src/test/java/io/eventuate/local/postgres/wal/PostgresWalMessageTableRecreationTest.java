package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("${SPRING_PROFILES_ACTIVE:PostgresWal,postgres}")
@SpringBootTest(classes = PostgresWalBinlogEntryReaderMessageTableTestConfiguration.class)
public class PostgresWalMessageTableRecreationTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterTableRecreation() {
    super.testNewMessageHandledAfterTableRecreation();
  }
}
