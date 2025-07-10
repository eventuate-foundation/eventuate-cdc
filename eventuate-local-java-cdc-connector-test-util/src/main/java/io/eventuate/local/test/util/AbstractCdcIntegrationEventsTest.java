package io.eventuate.local.test.util;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static io.eventuate.local.test.util.assertion.EventAssertOperationBuilder.fromEventInfo;

public abstract class AbstractCdcIntegrationEventsTest {
  @Autowired
  protected BinlogEntryReader binlogEntryReader;

  @Autowired
  protected TestHelper testHelper;

  @Test
  public void shouldGetEvents() {
    BinlogAssertion<PublishedEvent> eventAssertion = testHelper.prepareBinlogEntryHandlerEventAssertion(binlogEntryReader);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    EventInfo saveResult = testHelper.saveRandomEvent();
    EventInfo updateResult = testHelper.updateEvent(saveResult.getEntityId(), testHelper.generateId());

    eventAssertion.assertEventReceived(fromEventInfo(saveResult).build());
    eventAssertion.assertEventReceived(fromEventInfo(updateResult).build());

    binlogEntryReader.stop();
  }
}
