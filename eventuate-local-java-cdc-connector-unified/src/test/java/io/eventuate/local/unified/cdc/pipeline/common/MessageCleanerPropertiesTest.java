package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MessageCleanerPropertiesTest {

  private PropertyReader propertyReader = new PropertyReader();

  @Test
  public void testPropertiesWithPipeline() {
    String properties = "[{\"pipeline\" : \"somePipeline\"}]";

    MessageCleanerProperties messageCleanerProperties = convertProperties(properties);

    Assert.assertEquals("somePipeline", messageCleanerProperties.getPipeline());
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

    Assert.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assert.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assert.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assert.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assert.assertEquals("someSchema", messageCleanerProperties.getEventuateSchema());
    Assert.assertEquals(true, messageCleanerProperties.isMessageCleaningEnabled());
    Assert.assertEquals(1, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assert.assertEquals(true, messageCleanerProperties.isReceivedMessageCleaningEnabled());
    Assert.assertEquals(2, messageCleanerProperties.getReceivedMessagesMaxAgeInSeconds());
    Assert.assertEquals(3, messageCleanerProperties.getIntervalInSeconds());
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

    Assert.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assert.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assert.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assert.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assert.assertEquals(false, messageCleanerProperties.isMessageCleaningEnabled());
    Assert.assertEquals(2*24*60*60, messageCleanerProperties.getMessagesMaxAgeInSeconds());
    Assert.assertEquals(false, messageCleanerProperties.isReceivedMessageCleaningEnabled());
    Assert.assertEquals(2*24*60*60, messageCleanerProperties.getReceivedMessagesMaxAgeInSeconds());
    Assert.assertEquals(60, messageCleanerProperties.getIntervalInSeconds());
  }

  private MessageCleanerProperties convertProperties(String properties) {
    List<Map<String, Object>> propertyMaps = propertyReader.convertPropertiesToListOfMaps(properties);

    Assert.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    propertyReader.checkForUnknownProperties(props, MessageCleanerProperties.class);

    MessageCleanerProperties messageCleanerProperties = propertyReader
            .convertMapToPropertyClass(props, MessageCleanerProperties.class);

    messageCleanerProperties.validate();

    return messageCleanerProperties;
  }
}
