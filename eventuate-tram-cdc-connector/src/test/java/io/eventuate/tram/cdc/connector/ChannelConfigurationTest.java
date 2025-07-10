package io.eventuate.tram.cdc.connector;

import io.eventuate.messaging.activemq.spring.common.ChannelType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = ChannelConfigurationTest.Config.class)
public class ChannelConfigurationTest {
  @EnableConfigurationProperties(EventuateTramChannelProperties.class)
  public static class Config {
  }

  @Autowired
  private EventuateTramChannelProperties eventuateTramChannelProperties;

  @Test
  public void testPropertyParsing() {
    Assertions.assertEquals(2, eventuateTramChannelProperties.getChannelTypes().size());

    Assertions.assertEquals(ChannelType.QUEUE,
            eventuateTramChannelProperties.getChannelTypes().get("channel1"));

    Assertions.assertEquals(ChannelType.TOPIC,
            eventuateTramChannelProperties.getChannelTypes().get("channel2"));
  }
}
