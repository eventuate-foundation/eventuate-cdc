package io.eventuate.local.mysql.binlog;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(classes = ColumnOrderExtractorTest.Config.class)
public class ColumnOrderExtractorTest extends AbstractColumnOrderExtractorTest {

  @Configuration
  @EnableAutoConfiguration
  public static class Config {
  }
}
