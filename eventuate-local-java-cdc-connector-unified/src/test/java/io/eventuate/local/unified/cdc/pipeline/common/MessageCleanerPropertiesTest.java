package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class MessageCleanerPropertiesTest {

  private PropertyReader propertyReader = new PropertyReader();

  @Test
  public void testPropertiesWithPipeline() {
    String properties = "[{\"pipeline\" : \"somePipeline\"}]";

    MessageCleanerProperties messageCleanerProperties = convertProperties(properties);

    Assertions.assertEquals("somePipeline", messageCleanerProperties.getPipeline());
  }

  @Test
  public void testPropertiesWithoutPipeline() {

    String properties = "[{" +
            "\"dataSourceUrl\" : \"someUrl\"," +
            "\"dataSourceUserName\" : \"someUserName\"," +
            "\"dataSourcePassword\" : \"somePassword\"," +
            "\"dataSourceDriverClassName\" : \"someClass\"," +
            "\"eventuateSchema\" : \"someSchema\"," +
            "\"messageCleaningEnabled\" : true," +
            "\"messagesMaxAgeInSeconds\" : 1," +
            "\"receivedMessageCleaningEnabled\" : true," +
            "\"receivedMessagesMaxAgeInSeconds\" : 2," +
            "\"intervalInSeconds\" : 3" +
          "}]";

    MessageCleanerProperties messageCleanerProperties = convertProperties(properties);

    Assertions.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assertions.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assertions.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assertions.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assertions.assertEquals("someSchema", messageCleanerProperties.getEventuateSchema());
    Assertions.assertEquals(true, messageCleanerProperties.isMessageCleaningEnabled());
    Assertions.assertEquals(1, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assertions.assertEquals(true, messageCleanerProperties.isReceivedMessageCleaningEnabled());
    Assertions.assertEquals(2, messageCleanerProperties.getReceivedMessagesMaxAgeInSeconds());
    Assertions.assertEquals(3, messageCleanerProperties.getIntervalInSeconds());
  }

  @Test
  public void testDefaultProperties() {
    String properties = "[{" +
            "\"dataSourceUrl\" : \"someUrl\"," +
            "\"dataSourceUserName\" : \"someUserName\"," +
            "\"dataSourcePassword\" : \"somePassword\"," +
            "\"dataSourceDriverClassName\" : \"someClass\"" +
            "}]";

    MessageCleanerProperties messageCleanerProperties = convertProperties(properties);

    Assertions.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assertions.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assertions.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assertions.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assertions.assertEquals(false, messageCleanerProperties.isMessageCleaningEnabled());
    Assertions.assertEquals(2*24*60*60, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assertions.assertEquals(false, messageCleanerProperties.isReceivedMessageCleaningEnabled());
    Assertions.assertEquals(2*24*60*60, messageCleanerProperties.getReceivedMessagesMaxAgeInSeconds());
    Assertions.assertEquals(60, messageCleanerProperties.getIntervalInSeconds());
  }

  private MessageCleanerProperties convertProperties(String properties) {
    List<Map<String, Object>> propertyMaps = propertyReader.convertPropertiesToListOfMaps(properties);

    Assertions.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    propertyReader.checkForUnknownProperties(props, MessageCleanerProperties.class);

    MessageCleanerProperties messageCleanerProperties = propertyReader
            .convertMapToPropertyClass(props, MessageCleanerProperties.class);

    messageCleanerProperties.validate();

    return messageCleanerProperties;
  }
}
