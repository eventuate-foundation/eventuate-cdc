package io.eventuate.local.test.util;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractMessageTableMigrationTest {

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private BinlogEntryReader binlogEntryReader;

  @Autowired
  private IdGenerator idGenerator;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  private ConcurrentLinkedQueue<MessageWithDestination> messages;
  private String payload;

  public void testNewMessageHandledAfterColumnReordering() {
    messages = testHelper.prepareBinlogEntryHandlerMessageQueue(binlogEntryReader);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    sendMessage();
    assertMessageReceived();

    reorderMessageColumns();

    sendMessage();
    assertMessageReceived();

    binlogEntryReader.stop();
  }

  public void testNewMessageHandledAfterTableRecreation() {
    messages = testHelper.prepareBinlogEntryHandlerMessageQueue(binlogEntryReader);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    sendMessage();
    assertMessageReceived();

    recreateMessageTable();

    sendMessage();
    assertMessageReceived();

    binlogEntryReader.stop();
  }

  private void reorderMessageColumns() {
    jdbcTemplate.execute("ALTER TABLE eventuate.message drop destination;");
    jdbcTemplate.execute("ALTER TABLE eventuate.message drop payload;");
    jdbcTemplate.execute("ALTER TABLE eventuate.message add destination LONGTEXT;");
    jdbcTemplate.execute("ALTER TABLE eventuate.message add payload LONGTEXT;");
  }

  private void recreateMessageTable() {
    jdbcTemplate.execute("DROP TABLE eventuate.message;");

    jdbcTemplate.execute("CREATE TABLE message (\n" +
            "  id VARCHAR(767) PRIMARY KEY,\n" +
            "  headers LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
            "  published SMALLINT DEFAULT 0,\n" +
            "  creation_time BIGINT,\n" +
            "  destination LONGTEXT NOT NULL,\n" +
            "  payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL\n" +
            ");");
  }

  private void assertMessageReceived() {
    Eventually.eventually(() -> {
      MessageWithDestination message = messages.poll();
      //checking payload is enough, because in case of migration failure, cdc fails during message parsing, so message will not be delivered
      Assert.assertTrue(message.getPayload().contains(payload));
    });
  }

  private void sendMessage() {
    payload = "payload-" + testHelper.generateId();
    String rawPayload = "\"" + payload + "\"";

    testHelper.saveMessage(idGenerator, rawPayload, testHelper.generateId(), Collections.emptyMap(), eventuateSchema);
  }
}
