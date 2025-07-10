package io.eventuate.local.polling;

import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = {CustomDBTestConfiguration.class, PollingIntegrationTestConfiguration.class})
public class PollingCdcProcessorCustomDBEventsTest extends CdcProcessorEventsTest {
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
