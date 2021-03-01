package io.eventuate.local.test.util;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.MessageCleaner;
import io.eventuate.util.test.async.Eventually;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractMessageCleanerTest {

  @Configuration
  @EnableAutoConfiguration
  @Import(SqlDialectConfiguration.class)
  public static class Config {
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  @Autowired
  private DataSource dataSource;

  @Value("${spring.datasource.driver-class-name}")
  private String driver;

  private MessageCleaner messageCleaner;

  @Test
  public void testThatMessagePurged() {
    String messageId = insertMessages(true, System.currentTimeMillis() - 1000);
    assertTrue(messageExists(messageId));
    assertTrue(receivedMessageExists(messageId));

    createAndStartMessageCleaner(1);

    Eventually.eventually(() -> {
      assertFalse(messageExists(messageId));
      assertFalse(receivedMessageExists(messageId));
    });
  }

  @Test
  public void testThatOnlyOldPublishedMessagesPurgedAndPurgeIsPeriodic() {
    String notPublishedMessageId = insertMessages(false, System.currentTimeMillis() - 4000);
    String oldMessageId = insertMessages(true, System.currentTimeMillis() - 4000);
    String messageId = insertMessages(true, System.currentTimeMillis());

    assertTrue(messageExists(notPublishedMessageId));
    assertTrue(receivedMessageExists(notPublishedMessageId));
    assertTrue(messageExists(oldMessageId));
    assertTrue(receivedMessageExists(oldMessageId));
    assertTrue(messageExists(messageId));
    assertTrue(receivedMessageExists(messageId));

    createAndStartMessageCleaner(3);

    Eventually.eventually(() -> {
      assertTrue(messageExists(notPublishedMessageId));
      assertFalse(receivedMessageExists(notPublishedMessageId));

      assertFalse(messageExists(oldMessageId));
      assertFalse(receivedMessageExists(oldMessageId));

      assertTrue(messageExists(messageId));
      assertTrue(receivedMessageExists(messageId));
    });

    Eventually.eventually(() -> {
      assertTrue(messageExists(notPublishedMessageId));
      assertFalse(receivedMessageExists(notPublishedMessageId));

      assertFalse(messageExists(oldMessageId));
      assertFalse(receivedMessageExists(oldMessageId));

      assertFalse(messageExists(messageId));
      assertFalse(receivedMessageExists(messageId));
    });
  }

  @After
  public void cleanUp() {
    messageCleaner.stop();
  }

  private void createAndStartMessageCleaner(int age) {
    messageCleaner = new MessageCleaner(sqlDialectSelector.getDialect(driver),
            dataSource, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA), true, age, true, age, 1);

    messageCleaner.start();
  }

  private String insertMessages(boolean published, long creationTime) {
    String id = generateId();

    String sql = "INSERT INTO eventuate.message (id, destination, headers, payload, published, creation_time) " +
            "VALUES (?, 'CDC-IGNORED', '{}', '{}', ?, ?);";

    jdbcTemplate.update(sql, id, published ? 1 : 0, creationTime);

    sql = "INSERT INTO eventuate.received_messages (consumer_id, message_id, creation_time) VALUES (?, ?, ?)";

    jdbcTemplate.update(sql, generateId(), id, creationTime);

    return id;
  }

  private boolean messageExists(String id) {
    return jdbcTemplate.queryForList("select * from eventuate.message where id = ?", id).size() == 1;
  }

  private boolean receivedMessageExists(String id) {
    return jdbcTemplate.queryForList("select * from eventuate.received_messages where message_id = ?", id).size() == 1;
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }
}
