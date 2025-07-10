package io.eventuate.local.polling;

import io.eventuate.local.test.util.AbstractMessageTableMigrationTest;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = PollingBinlogEntryReaderMessageTableTestConfiguration.class)
public class PollingMessageTableRecreationTest extends AbstractMessageTableMigrationTest {

  @Override
  @Test
  public void testNewMessageHandledAfterTableRecreation() {
    super.testNewMessageHandledAfterTableRecreation();
  }
}
