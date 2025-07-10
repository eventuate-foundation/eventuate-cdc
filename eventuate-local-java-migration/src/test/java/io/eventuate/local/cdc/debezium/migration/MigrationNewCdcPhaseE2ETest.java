package io.eventuate.local.cdc.debezium.migration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.ExecutionException;

@SpringBootTest(classes = MigrationE2ETestConfiguration.class)
@DirtiesContext
public class MigrationNewCdcPhaseE2ETest extends AbstractE2EMigrationTest {

  @Test
  public void readEventFromTheOldCdc() throws InterruptedException, ExecutionException {
    Handler handler = new Handler();
    subscribe(handler);
    handler.assertContainsEvent();
    handler.assertContainsEventWithId(sendEvent());
  }
}
