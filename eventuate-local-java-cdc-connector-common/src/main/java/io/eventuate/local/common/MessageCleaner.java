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
  private MessageCleanerPurgeProperties messageCleanerPurgeProperties;

  private Timer timer;
  private JdbcTemplate jdbcTemplate;

  public MessageCleaner(EventuateSqlDialect eventuateSqlDialect,
                        DataSource dataSource,
                        EventuateSchema eventuateSchema,
                        MessageCleanerPurgeProperties messageCleanerPurgeProperties) {
    this.eventuateSqlDialect = eventuateSqlDialect;
    this.eventuateSchema = eventuateSchema;
    this.messageCleanerPurgeProperties = messageCleanerPurgeProperties;

    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  public void start() {
    if (messageCleanerPurgeProperties.isPurgeMessagesEnabled() ||
            messageCleanerPurgeProperties.isPurgeReceivedMessagesEnabled()) {
      timer = new Timer();

      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          cleanTables();
        }
      }, 0, messageCleanerPurgeProperties.getPurgeIntervalInSeconds() * 1000);
    }
  }

  public void stop() {
    if (timer != null) {
      timer.cancel();
    }
  }

  private void cleanTables() {
    if (messageCleanerPurgeProperties.isPurgeMessagesEnabled()) {
      cleanMessages();
    }

    if (messageCleanerPurgeProperties.isPurgeReceivedMessagesEnabled()) {
      cleanReceivedMessages();
    }
  }

  private void cleanMessages() {
    String table = eventuateSchema.qualifyTable("message");

    String sql = String.format("delete from %s where %s - creation_time > ? and published = 1",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messageCleanerPurgeProperties.getPurgeMessagesMaxAgeInSeconds() * 1000);
  }

  private void cleanReceivedMessages() {
    String table = eventuateSchema.qualifyTable("received_messages");

    String sql = String.format("delete from %s where %s - creation_time > ?",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messageCleanerPurgeProperties.getPurgeReceivedMessagesMaxAgeInSeconds() * 1000);
  }
}
