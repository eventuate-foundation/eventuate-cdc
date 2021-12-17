package io.eventuate.local.polling;

import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {CustomDBTestConfiguration.class, PollingIntegrationTestConfiguration.class})
public class PollingCdcProcessorCustomDBEventsTest extends CdcProcessorEventsTest {
  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor eventuateLocalCustomDBSqlEditor;

  @Before
  public void createCustomDB() {
    customDBCreator.create(eventuateLocalCustomDBSqlEditor);
  }

  public void testCdcProcessingStatusService() {
    //do nothing
  }
}
