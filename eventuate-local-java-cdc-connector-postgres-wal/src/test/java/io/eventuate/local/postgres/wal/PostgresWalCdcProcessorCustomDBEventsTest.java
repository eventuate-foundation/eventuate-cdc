package io.eventuate.local.postgres.wal;


import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.CustomDBCreator;
import io.eventuate.local.testutil.CustomDBTestConfiguration;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import io.eventuate.local.testutil.SqlScriptEditor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPostgresWalProfilesResolver.class)
@SpringBootTest(classes = {CustomDBTestConfiguration.class, PostgresWalCdcIntegrationTestConfiguration.class})
public class PostgresWalCdcProcessorCustomDBEventsTest extends CdcProcessorEventsTest {

  @Autowired
  private CustomDBCreator customDBCreator;

  @Autowired
  private SqlScriptEditor sqlScriptEditor;

  @BeforeEach
  public void createCustomDB() {
    customDBCreator.create(sqlScriptEditor);
  }

  @Test
  public void testCdcProcessingStatusService() {
    //do nothing
  }
}
