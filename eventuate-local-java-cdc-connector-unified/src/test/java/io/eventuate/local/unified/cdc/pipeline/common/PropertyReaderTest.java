package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PropertyReaderTest {

  private PropertyReader propertyReader = new PropertyReader();

  @Test
  public void testCorrectProperties() {
    List<Map<String, Object>> propertyMaps = propertyReader
            .convertPropertiesToListOfMaps("[{\"type\" : \"mysql\", \"reader\" : \"reader1\"}]");

    Assertions.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    propertyReader.checkForUnknownProperties(props, CdcPipelineProperties.class);

    CdcPipelineProperties cdcPipelineProperties = propertyReader
            .convertMapToPropertyClass(props, CdcPipelineProperties.class);

    Assertions.assertEquals("mysql", cdcPipelineProperties.getType());
    Assertions.assertEquals("reader1", cdcPipelineProperties.getReader());
  }

  @Test
  public void testUnknownProperties() {
    List<Map<String, Object>> propertyMaps = propertyReader
            .convertPropertiesToListOfMaps("[{\"type\" : \"mysql\", \"reader\" : \"reader1\", \"somepropname\" : \"somepropvalue\"}]");

    Assertions.assertEquals(1, propertyMaps.size());

    Map<String, Object> props = propertyMaps.get(0);

    Exception exception = null;

    try {
      propertyReader.checkForUnknownProperties(props, CdcPipelineProperties.class);
    } catch (Exception e) {
      exception = e;
    }

    Assertions.assertNotNull(exception);
    Assertions.assertEquals("Unknown properties: [somepropname] for class io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties", exception.getMessage());
  }
}
