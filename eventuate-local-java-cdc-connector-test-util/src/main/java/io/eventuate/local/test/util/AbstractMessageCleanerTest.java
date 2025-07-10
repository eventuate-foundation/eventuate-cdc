package io.eventuate.local.test.util;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.unified.cdc.pipeline.MessageCleaner;
import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  public void testThatMessageCleaned() {
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
  public void testThatOnlyOldMessagesCleanedAndCleaningIsPeriodic() {
    String oldMessageId = insertMessages(true, System.currentTimeMillis() - 4000);
    String messageId = insertMessages(true, System.currentTimeMillis());

    assertTrue(messageExists(oldMessageId));
    assertTrue(receivedMessageExists(oldMessageId));
    assertTrue(messageExists(messageId));
    assertTrue(receivedMessageExists(messageId));

    createAndStartMessageCleaner(3);

    Eventually.eventually(() -> {
      assertFalse(messageExists(oldMessageId));
      assertFalse(receivedMessageExists(oldMessageId));

      assertTrue(messageExists(messageId));
      assertTrue(receivedMessageExists(messageId));
    });

    Eventually.eventually(() -> {
      assertFalse(messageExists(oldMessageId));
      assertFalse(receivedMessageExists(oldMessageId));

      assertFalse(messageExists(messageId));
      assertFalse(receivedMessageExists(messageId));
    });
  }

  @AfterEach
  public void cleanUp() {
    messageCleaner.stop();
  }

  private void createAndStartMessageCleaner(int age) {
    MessageCleanerProperties messageCleaningProperties = new MessageCleanerProperties();

    messageCleaningProperties.setIntervalInSeconds(1);
    messageCleaningProperties.setMessageCleaningEnabled(true);
    messageCleaningProperties.setReceivedMessageCleaningEnabled(true);
    messageCleaningProperties.setMessagesMaxAgeInSeconds(age);
    messageCleaningProperties.setReceivedMessagesMaxAgeInSeconds(age);

    messageCleaner = new MessageCleaner(sqlDialectSelector.getDialect(driver),
            dataSource, new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA), messageCleaningProperties);

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
