package io.eventuate.local.test.util;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.CdcProcessingStatusService;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

import static io.eventuate.local.test.util.assertion.EventAssertOperationBuilder.fromEventInfo;

public abstract class CdcProcessorEventsTest implements CdcProcessorCommon {

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected IdGenerator idGenerator;

  @Autowired
  protected BinlogEntryReader binlogEntryReader;

  @Test
  public void shouldReadNewEventsOnly() {
    BinlogAssertion<PublishedEvent> binlogAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader, this::onEventSent);

    startEventProcessing();

    EventInfo eventInfo = testHelper.saveRandomEvent();

    binlogAssertion.assertEventReceived(fromEventInfo(eventInfo).build());

    stopEventProcessing();

    binlogAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader, this::onEventSent);

    startEventProcessing();

    EventInfo newEventInfo = testHelper.updateEvent(eventInfo.getEntityId(), eventInfo.getEventData());

    binlogAssertion
            .assertEventReceived(fromEventInfo(newEventInfo)
                    .excludeId(eventInfo.getEventId())
                    .build());

    stopEventProcessing();
  }

  @Test
  public void shouldReadUnprocessedEventsAfterStartup() {
    EventInfo eventInfo = testHelper.saveRandomEvent();

    BinlogAssertion<PublishedEvent> binlogAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader);
    startEventProcessing();

    binlogAssertion.assertEventReceived(fromEventInfo(eventInfo).build());

    stopEventProcessing();
  }


  @Test
  public void testCdcProcessingStatusService() {
    testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader, event -> {
      onEventSent(event);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    startEventProcessing();

    for (int i = 0; i < 3; i++) {
      testHelper.saveRandomEvent();
    }

    CdcProcessingStatusService pollingProcessingStatusService = binlogEntryReader.getCdcProcessingStatusService();

    Assertions.assertFalse(pollingProcessingStatusService.getCurrentStatus().isCdcProcessingFinished());

    Eventually.eventually(60,
            500,
            TimeUnit.MILLISECONDS,
            () -> Assertions.assertTrue(pollingProcessingStatusService.getCurrentStatus().isCdcProcessingFinished()));

    stopEventProcessing();
  }

  protected void startEventProcessing() {
    testHelper.runInSeparateThread(binlogEntryReader::start);
  }

  protected void stopEventProcessing() {
    binlogEntryReader.stop();
  }
}
