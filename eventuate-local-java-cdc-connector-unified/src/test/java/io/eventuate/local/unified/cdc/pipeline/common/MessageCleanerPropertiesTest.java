package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MessageCleanerPropertiesTest {

  private PropertyReader propertyReader = new PropertyReader();

  @Test
  public void testFullProperties() {

    String properties = "[{" +
            "\"dataSourceUrl\" : \"someUrl\"," +
            "\"dataSourceUserName\" : \"someUserName\"," +
            "\"dataSourcePassword\" : \"somePassword\"," +
            "\"dataSourceDriverClassName\" : \"someClass\"," +
            "\"eventuateSchema\" : \"someSchema\"," +
            "\"purgeMessagesEnabled\" : true," +
            "\"purgeMessagesMaxAgeInSeconds\" : 1," +
            "\"purgeReceivedMessagesEnabled\" : true," +
            "\"purgeReceivedMessagesMaxAgeInSeconds\" : 2," +
            "\"purgeIntervalInSeconds\" : 3" +
            "}]";



    List<Map<String, Object>> propertyMaps = propertyReader.convertPropertiesToListOfMaps(properties);

    Assert.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    propertyReader.checkForUnknownProperties(props, MessageCleanerProperties.class);

    MessageCleanerProperties messageCleanerProperties = propertyReader
            .convertMapToPropertyClass(props, MessageCleanerProperties.class);

    Assert.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assert.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assert.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assert.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assert.assertEquals("someSchema", messageCleanerProperties.getEventuateSchema());
    Assert.assertEquals(true, messageCleanerProperties.getPurgeMessagesEnabled());
    Assert.assertEquals(1, messageCleanerProperties.getPurgeMessagesMaxAgeInSeconds());
    Assert.assertEquals(true, messageCleanerProperties.getPurgeReceivedMessagesEnabled());
    Assert.assertEquals(2, messageCleanerProperties.getPurgeReceivedMessagesMaxAgeInSeconds());
    Assert.assertEquals(3, messageCleanerProperties.getPurgeIntervalInSeconds());
  }

  @Test
  public void testDefaultProperties() {
    String properties = "[{" +
            "\"dataSourceUrl\" : \"someUrl\"," +
            "\"dataSourceUserName\" : \"someUserName\"," +
            "\"dataSourcePassword\" : \"somePassword\"," +
            "\"dataSourceDriverClassName\" : \"someClass\"" +
            "}]";

    List<Map<String, Object>> propertyMaps = propertyReader.convertPropertiesToListOfMaps(properties);

    Assert.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    propertyReader.checkForUnknownProperties(props, MessageCleanerProperties.class);

    MessageCleanerProperties messageCleanerProperties = propertyReader
            .convertMapToPropertyClass(props, MessageCleanerProperties.class);

    Assert.assertEquals("someUrl", messageCleanerProperties.getDataSourceUrl());
    Assert.assertEquals("someUserName", messageCleanerProperties.getDataSourceUserName());
    Assert.assertEquals("somePassword", messageCleanerProperties.getDataSourcePassword());
    Assert.assertEquals("someClass", messageCleanerProperties.getDataSourceDriverClassName());
    Assert.assertEquals(false, messageCleanerProperties.getPurgeMessagesEnabled());
    Assert.assertEquals(2*24*60*60, messageCleanerProperties.getPurgeMessagesMaxAgeInSeconds());
    Assert.assertEquals(false, messageCleanerProperties.getPurgeReceivedMessagesEnabled());
    Assert.assertEquals(2*24*60*60, messageCleanerProperties.getPurgeReceivedMessagesMaxAgeInSeconds());
    Assert.assertEquals(60, messageCleanerProperties.getPurgeIntervalInSeconds());
  }
}
