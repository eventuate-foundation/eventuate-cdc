package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
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

    Assert.assertEquals("jdbc:postgresql://postgreswalpipeline/eventuate", messageCleanerProperties.getDataSourceUrl());
    Assert.assertEquals("eventuate", messageCleanerProperties.getDataSourceUserName());
    Assert.assertEquals("eventuate", messageCleanerProperties.getDataSourcePassword());
    Assert.assertEquals("org.postgresql.Driver", messageCleanerProperties.getDataSourceDriverClassName());
    Assert.assertTrue(messageCleanerProperties.isMessageCleaningEnabled());
    Assert.assertEquals(1, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assert.assertEquals(1, messageCleanerProperties.getIntervalInSeconds());
  }
}
