package io.eventuate.local.test.util;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.test.util.assertion.BinlogAssertion;
import io.eventuate.tram.cdc.connector.MessageWithDestination;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;

import static io.eventuate.local.test.util.assertion.MessageAssertOperationBuilder.assertion;

public abstract class AbstractMessageTableMigrationTest {

  @Autowired
  private Environment env;

  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @Autowired
  private TestHelper testHelper;

  @Autowired
  private EventuateSchema eventuateSchema;

  @Autowired
  private BinlogEntryReader binlogEntryReader;

  @Autowired
  private IdGenerator idGenerator;

  private BinlogAssertion<MessageWithDestination> messageAssertion;
  private String rawPayload;

  public void testNewMessageHandledAfterColumnReordering() {
    executeMigrationTest(this::reorderMessageColumns);
  }

  public void testNewMessageHandledAfterTableRecreation() {
    executeMigrationTest(this::recreateMessageTable);
  }

  private void executeMigrationTest(Runnable migrationCallback) {
    messageAssertion = testHelper.prepareBinlogEntryHandlerMessageAssertion(binlogEntryReader);

    testHelper.runInSeparateThread(binlogEntryReader::start);

    sendMessage();
    assertMessageReceived();

    migrationCallback.run();

    sendMessage();
    assertMessageReceived();

    binlogEntryReader.stop();
  }

  private void reorderMessageColumns() {
    executeSql(loadColumnReorderingSql());
  }

  private void recreateMessageTable() {
    executeSql(loadTableRecreationSql());
  }

  private void assertMessageReceived() {
    messageAssertion.assertEventReceived(assertion().withPayload(rawPayload).build());
  }

  private String sendMessage() {
    rawPayload = "\"" + "payload-" + testHelper.generateId() + "\"";

    return testHelper.saveMessage(idGenerator, rawPayload, testHelper.generateId(), Collections.emptyMap(), eventuateSchema);
  }

  private void executeSql(String sql) {
    Arrays.stream(sql.split(";")).forEach(jdbcTemplate::execute);
  }

  private String loadColumnReorderingSql() {
    return loadResource(String.format("/reorder_message_columns.%s.sql", getDatabase()));
  }

  private String loadTableRecreationSql() {
    return loadResource(String.format("/recreate_message_table.%s.sql", getDatabase()));
  }

  private String getDatabase() {
    String[] databaseProfiles = {"postgres", "mssql"};

    for (String database : databaseProfiles) {
      if (Arrays.stream(env.getActiveProfiles()).anyMatch(database::equals)) {
        return database;
      }
    }

    return "mysql";
  }

  private String loadResource(String resource) {
    try (InputStream resourceStream = getClass().getResourceAsStream(resource)) {
      Scanner scanner = new Scanner(resourceStream);
      scanner.useDelimiter("//Z");
      return scanner.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
