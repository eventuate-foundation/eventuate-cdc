package io.eventuate.local.polling;

import io.eventuate.local.common.CdcProcessingStatusService;
import io.eventuate.testutil.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.TimeUnit;

@ActiveProfiles("EventuatePolling")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingCdcProcessorEventsTest extends AbstractPollingCdcProcessorEventsTest {
  @Test
  public void testPollingCdcProcessingStatusService() {

    prepareBinlogEntryHandler(publishedEvent -> {
      onEventSent(publishedEvent);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    startEventProcessing();

    saveEvent(generateTestCreatedEvent());
    saveEvent(generateTestCreatedEvent());
    saveEvent(generateTestCreatedEvent());

    CdcProcessingStatusService pollingProcessingStatusService = pollingDao.getCdcProcessingStatusService();

    Assert.assertFalse(pollingProcessingStatusService.getCurrentStatus().isCdcProcessingFinished());

    Eventually.eventually(60,
            500,
            TimeUnit.MILLISECONDS,
            () -> Assert.assertTrue(pollingProcessingStatusService.getCurrentStatus().isCdcProcessingFinished()));

    stopEventProcessing();
  }
}
