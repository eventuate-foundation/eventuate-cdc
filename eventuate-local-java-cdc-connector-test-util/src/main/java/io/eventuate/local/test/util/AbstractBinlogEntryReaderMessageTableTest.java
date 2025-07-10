package io.eventuate.local.test.util;

import com.google.common.collect.ImmutableMap;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static io.eventuate.local.test.util.assertion.MessageAssertOperationBuilder.assertion;

public abstract class AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected EventuateSchema eventuateSchema;

  @Autowired
  protected BinlogEntryReader binlogEntryReader;

  @Autowired
  protected IdGenerator idGenerator;

  @Test
  public void testMessageHandled() {

    BinlogAssertion<MessageWithDestination> binlogAssertion = testHelper.prepareBinlogEntryHandlerMessageAssertion(binlogEntryReader);

    String payload = "\"" + "payload-" + testHelper.generateId() + "\"";
    String destination = "destination-" + testHelper.generateId();
    Map<String, String> headers = ImmutableMap.of("key", "value");

    String messageId = testHelper.saveMessage(idGenerator, payload, destination, headers, eventuateSchema);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    binlogAssertion
            .assertEventReceived(
                    assertion()
                            .withId(messageId)
                            .withDestination(destination)
                            .withPayload(payload)
                            .withHeaders(headers)
                            .build());

    binlogEntryReader.stop();
  }
}
