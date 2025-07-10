package io.eventuate.local.mysql.binlog;

import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {CustomDBTestConfiguration.class,
        MySqlBinlogCdcIntegrationTestConfiguration.class,
        OffsetStoreMockConfiguration.class})
public class MySQLCdcProcessorCustomDBEventsTest extends AbstractMySQLCdcProcessorEventsTest {

  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor eventuateLocalCustomDBSqlEditor;

  @BeforeEach
  public void createCustomDB() {
    customDBCreator.create(eventuateLocalCustomDBSqlEditor);
  }

  @Test
  public void testCdcProcessingStatusService() {
    //do nothing
  }
}
