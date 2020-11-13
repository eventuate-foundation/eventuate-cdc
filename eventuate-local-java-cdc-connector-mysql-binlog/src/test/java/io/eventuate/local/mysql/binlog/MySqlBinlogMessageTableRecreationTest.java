package io.eventuate.local.mysql.binlog;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MySqlBinlogEntryReaderMessageTableTestConfiguration.class)
public class MySqlBinlogMessageTableRecreationTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterTableRecreation() {
    super.testNewMessageHandledAfterTableRecreation();
  }
}
