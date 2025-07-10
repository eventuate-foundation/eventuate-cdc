package io.eventuate.cdc.e2e.common;

import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@SpringBootTest(classes = {MessageCleanerE2ETest.Config.class})
public class MessageCleanerE2ETest {

  @Configuration
  @EnableAutoConfiguration
  public static class Config {
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }
  }

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Test
  public void assertMessagesCleaned() throws Exception {
    Eventually.eventually(() ->
      Assertions.assertEquals(0, jdbcTemplate.queryForList("select * from eventuate.message").size()));
  }
}
