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
  private MessageCleaningProperties messageCleaningProperties;

  private Timer timer;
  private JdbcTemplate jdbcTemplate;

  public MessageCleaner(EventuateSqlDialect eventuateSqlDialect,
                        DataSource dataSource,
                        EventuateSchema eventuateSchema,
                        MessageCleaningProperties messageCleaningProperties) {
    this.eventuateSqlDialect = eventuateSqlDialect;
    this.eventuateSchema = eventuateSchema;
    this.messageCleaningProperties = messageCleaningProperties;

    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  public void start() {
    if (messageCleaningProperties.isMessagesEnabled() ||
            messageCleaningProperties.isReceivedMessagesEnabled()) {
      timer = new Timer();

      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          cleanTables();
        }
      }, 0, messageCleaningProperties.getIntervalInSeconds() * 1000);
    }
  }

  public void stop() {
    if (timer != null) {
      timer.cancel();
    }
  }

  private void cleanTables() {
    if (messageCleaningProperties.isMessagesEnabled()) {
      cleanMessages();
    }

    if (messageCleaningProperties.isReceivedMessagesEnabled()) {
      cleanReceivedMessages();
    }
  }

  private void cleanMessages() {
    String table = eventuateSchema.qualifyTable("message");

    String sql = String.format("delete from %s where %s - creation_time > ?",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messageCleaningProperties.getMessagesMaxAgeInSeconds() * 1000);
  }

  private void cleanReceivedMessages() {
    String table = eventuateSchema.qualifyTable("received_messages");

    String sql = String.format("delete from %s where %s - creation_time > ?",
            table, eventuateSqlDialect.getCurrentTimeInMillisecondsExpression());

    jdbcTemplate.update(sql, messageCleaningProperties.getReceivedMessagesMaxAgeInSeconds() * 1000);
  }
}
