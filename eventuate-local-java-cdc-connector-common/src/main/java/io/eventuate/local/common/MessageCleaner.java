package io.eventuate.local.common;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Timer;
import java.util.TimerTask;

public class MessageCleaner {

  private EventuateSqlDialect eventuateSqlDialect;
  private DataSource dataSource;
  private EventuateSchema eventuateSchema;
  private Boolean purgeMessagesEnabled;
  private int purgeMessagesMaxAgeInSeconds;
  private Boolean purgeReceivedMessagesEnabled;
  private int purgeReceivedMessagesMaxAgeInSeconds;

  private int purgeIntervalInSeconds;

  private Timer timer;
  private JdbcTemplate jdbcTemplate;


  public MessageCleaner(EventuateSqlDialect eventuateSqlDialect,
                        DataSource dataSource,
                        EventuateSchema eventuateSchema,
                        Boolean purgeMessagesEnabled,
                        int purgeMessagesMaxAgeInSeconds,
                        Boolean purgeReceivedMessagesEnabled,
                        int purgeReceivedMessagesMaxAgeInSeconds,
                        int purgeIntervalInSeconds) {
    this.eventuateSqlDialect = eventuateSqlDialect;
    this.dataSource = dataSource;
    this.eventuateSchema = eventuateSchema;
    this.purgeMessagesEnabled = purgeMessagesEnabled;
    this.purgeMessagesMaxAgeInSeconds = purgeMessagesMaxAgeInSeconds;
    this.purgeReceivedMessagesEnabled = purgeReceivedMessagesEnabled;
    this.purgeReceivedMessagesMaxAgeInSeconds = purgeReceivedMessagesMaxAgeInSeconds;
    this.purgeIntervalInSeconds = purgeIntervalInSeconds;

    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  public void start() {
    if (purgeMessagesEnabled || purgeReceivedMessagesEnabled) {
      timer = new Timer();

      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          cleanTables();
        }
      }, 0, purgeIntervalInSeconds * 1000);
    }
  }

  public void stop() {
    if (timer != null) {
      timer.cancel();
    }
  }

  private void cleanTables() {
    if (purgeMessagesEnabled) {
      cleanMessages();
    }

    if (purgeReceivedMessagesEnabled) {
      cleanReceivedMessages();
    }
  }

  private void cleanMessages() {
    String table = eventuateSchema.qualifyTable("message");

    String sql = "delete from " + table +
            " where " + eventuateSqlDialect.getCurrentTimeInMillisecondsExpression() + " - creation_time > " + (purgeMessagesMaxAgeInSeconds * 1000) +
            " and published = 1";

    jdbcTemplate.execute(sql);
  }

  private void cleanReceivedMessages() {
    String table = eventuateSchema.qualifyTable("received_messages");

    String sql = "delete from " + table +
            " where " + eventuateSqlDialect.getCurrentTimeInMillisecondsExpression() + " - creation_time > " + (purgeMessagesMaxAgeInSeconds * 1000);

    jdbcTemplate.execute(sql);
  }
}
