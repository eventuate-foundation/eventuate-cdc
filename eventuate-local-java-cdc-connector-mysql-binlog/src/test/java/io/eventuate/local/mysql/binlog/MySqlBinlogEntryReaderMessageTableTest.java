package io.eventuate.local.mysql.binlog;

import com.google.common.collect.ImmutableSet;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.test.util.AbstractBinlogEntryReaderMessageTableTest;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.eventuate.local.test.util.assertion.MessageAssertOperationBuilder.assertion;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MySqlBinlogEntryReaderMessageTableTestConfiguration.class)
public class MySqlBinlogEntryReaderMessageTableTest extends AbstractBinlogEntryReaderMessageTableTest {

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private OffsetStoreMock offsetStoreMock;

  private String messageId1;
  private String messageId2;

  private String payload1;
  private String payload2;

  private List<BinlogFileOffset> offsets;
  private BinlogAssertion<MessageWithDestination> binlogAssertion;

  @Test
  public void testProcessingRestartInMiddleOfTransaction() {
    prepareInitialAssertion();

    insertMessagesInsideTransaction();

    testHelper.runInSeparateThread(binlogEntryReader::start);

    assertEventsPublished();

    restartReaderAndConnectAtMiddleOfTransaction();

    assertSecondEventPublished();

    binlogEntryReader.stop();
  }

  private void prepareInitialAssertion() {
    offsets = new CopyOnWriteArrayList<>();

    payload1 = testHelper.generateRandomPayload();
    payload2 = testHelper.generateRandomPayload();

    binlogAssertion = testHelper.prepareBinlogEntryHandlerMessageAssertion(binlogEntryReader,
            message -> {
              if (ImmutableSet.of(payload1, payload2).contains(message.getPayload())) {
                offsets.add(message.getBinlogFileOffset().get());
              }
            });
  }

  private void insertMessagesInsideTransaction() {
    transactionTemplate.executeWithoutResult(transactionStatus -> {
      messageId1 = testHelper.saveMessage(idGenerator,
              payload1, testHelper.generateId(), Collections.emptyMap(), eventuateSchema);

      messageId2 = testHelper.saveMessage(idGenerator,
              payload2, testHelper.generateId(), Collections.emptyMap(), eventuateSchema);
    });
  }

  private void restartReaderAndConnectAtMiddleOfTransaction() {
    binlogEntryReader.stop();

    offsetStoreMock.binlogFileOffset = Optional.of(offsets.get(0));

    binlogAssertion = testHelper.prepareBinlogEntryHandlerMessageAssertion(binlogEntryReader);

    testHelper.runInSeparateThread(binlogEntryReader::start);
  }

  private void assertEventsPublished() {
    assertEventPublished(messageId1, payload1);
    assertEventPublished(messageId2, payload2);

    Eventually.eventually(() -> Assert.assertEquals(2, offsets.size()));
  }

  private void assertSecondEventPublished() {
    assertEventPublished(messageId2, payload2);
  }

  private void assertEventPublished(String messageId, String payload) {
    binlogAssertion
            .assertEventReceived(
                    assertion()
                            .withId(messageId)
                            .withPayload(payload)
                            .build());
  }
}
