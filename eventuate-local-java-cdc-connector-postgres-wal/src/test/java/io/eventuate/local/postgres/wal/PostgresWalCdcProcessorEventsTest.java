package io.eventuate.local.postgres.wal;

import io.eventuate.local.common.CdcProcessingStatus;
import io.eventuate.local.common.CdcProcessingStatusService;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.TimeUnit;

@ActiveProfiles("PostgresWal")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PostgresWalCdcIntegrationTestConfiguration.class)
public class PostgresWalCdcProcessorEventsTest extends AbstractPostgresWalCdcProcessorEventsTest {

  @Test
  public void testPostgresWalCdcProcessingStatusService() {

    prepareBinlogEntryHandler(publishedEvent -> {
      onEventSent(publishedEvent);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    startEventProcessing();

    testHelper.saveEvent(testHelper.generateTestCreatedEvent());
    testHelper.saveEvent(testHelper.generateTestCreatedEvent());
    testHelper.saveEvent(testHelper.generateTestCreatedEvent());

    CdcProcessingStatusService cdcProcessingStatusService = postgresWalClient.getCdcProcessingStatusService();

    Assert.assertFalse(cdcProcessingStatusService.getCurrentStatus().isCdcProcessingFinished());

    Eventually.eventually(60,
            500,
            TimeUnit.MILLISECONDS,
            () -> {
              CdcProcessingStatus currentStatus = cdcProcessingStatusService.getCurrentStatus();
              Assert.assertTrue(currentStatus.toString(), currentStatus.isCdcProcessingFinished());
            });

    stopEventProcessing();
  }
}
