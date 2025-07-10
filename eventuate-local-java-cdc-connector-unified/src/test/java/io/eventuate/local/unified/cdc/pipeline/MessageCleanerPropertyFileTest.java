package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

@SpringBootTest(classes = MessageCleanerPropertyFileTest.Config.class)
@ActiveProfiles("cleanertest")
public class MessageCleanerPropertyFileTest {
  @Configuration
  @EnableConfigurationProperties(RawUnifiedCdcProperties.class)
  public static class Config {
  }

  @Autowired
  private RawUnifiedCdcProperties rawUnifiedCdcProperties;

  @Test
  public void testPropertiesParsed() {
    Map<String, Object> properties = rawUnifiedCdcProperties.getCleaner().get("test");

    MessageCleanerProperties messageCleanerProperties = new CdcMessageCleanerConfigurator().prepareMessageCleanerProperties(properties);

    Assertions.assertEquals("jdbc:postgresql://postgreswalpipeline/eventuate", messageCleanerProperties.getDataSourceUrl());
    Assertions.assertEquals("eventuate", messageCleanerProperties.getDataSourceUserName());
    Assertions.assertEquals("eventuate", messageCleanerProperties.getDataSourcePassword());
    Assertions.assertEquals("org.postgresql.Driver", messageCleanerProperties.getDataSourceDriverClassName());
    Assertions.assertTrue(messageCleanerProperties.isMessageCleaningEnabled());
    Assertions.assertEquals(1, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assertions.assertEquals(1, messageCleanerProperties.getIntervalInSeconds());
  }
}
