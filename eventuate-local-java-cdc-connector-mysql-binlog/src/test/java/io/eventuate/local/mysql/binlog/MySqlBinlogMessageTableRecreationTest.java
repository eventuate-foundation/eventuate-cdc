package io.eventuate.local.mysql.binlog;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = MySqlBinlogEntryReaderMessageTableTestConfiguration.class)
public class MySqlBinlogMessageTableRecreationTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterTableRecreation() {
    super.testNewMessageHandledAfterTableRecreation();
  }
}
