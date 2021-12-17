package io.eventuate.local.postgres.wal;


import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {CustomDBTestConfiguration.class, PostgresWalCdcIntegrationTestConfiguration.class})
public class PostgresWalCdcProcessorCustomDBEventsTest extends CdcProcessorEventsTest {

  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor sqlScriptEditor;

  @Before
  public void createCustomDB() {
    customDBCreator.create(sqlScriptEditor);
  }

  public void testCdcProcessingStatusService() {
    //do nothing
  }
}
