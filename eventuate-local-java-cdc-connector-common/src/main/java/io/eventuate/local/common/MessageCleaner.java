package io.eventuate.local.common;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Timer;
import java.util.TimerTask;

public class MessageCleaner {

  private EventuateSqlDialect eventuateSqlDialect;
  private EventuateSchema eventuateSchema;
  private MessagePurgeProperties messagePurgeProperties;

  private Timer timer;
  private JdbcTemplate jdbcTemplate;

  public MessageCleaner(EventuateSqlDialect eventuateSqlDialect,
                        DataSource dataSource,
                        EventuateSchema eventuateSchema,
                        MessagePurgeProperties messagePurgeProperties) {
    this.eventuateSqlDialect = eventuateSqlDialect;
    this.eventuateSchema = eventuateSchema;
    this.messagePurgeProperties = messagePurgeProperties;

    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  public void start() {
    if (messagePurgeProperties.isMessagesEnabled() ||
            messagePurgeProperties.isReceivedMessagesEnabled()) {
      timer = new Timer();

      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          cleanTables();
        }
      }, 0, messagePurgeProperties.getIntervalInSeconds() * 1000);
    }
  }

  public void stop() {
    if (timer != null) {
      timer.cancel();
    }
  }

  private void cleanTables() {
    if (messagePurgeProperties.isMessagesEnabled()) {
      cleanMessages();
    }

    if (messagePurgeProperties.isReceivedMessagesEnabled()) {
      cleanReceivedMessages();
    }
  }

  private void cleanMessages() {
    String table = eventuateSchema.qualifyTable("message");

//TODO: use this query when wip-db-id-gen merged to master, it has change that makes all cdc types mark processed messages as published
//    String sql = String.format("delete from %s where %s - creation_time > ? and published = 1",
//            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    String sql = String.format("delete from %s where %s - creation_time > ?",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messagePurgeProperties.getMessagesMaxAgeInSeconds() * 1000);
  }

  private void cleanReceivedMessages() {
    String table = eventuateSchema.qualifyTable("received_messages");

    String sql = String.format("delete from %s where %s - creation_time > ?",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messagePurgeProperties.getReceivedMessagesMaxAgeInSeconds() * 1000);
  }
}
