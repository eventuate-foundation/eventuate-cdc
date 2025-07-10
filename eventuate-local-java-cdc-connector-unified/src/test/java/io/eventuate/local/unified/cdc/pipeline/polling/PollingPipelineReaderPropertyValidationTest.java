package io.eventuate.local.unified.cdc.pipeline.polling;

import io.eventuate.local.unified.cdc.pipeline.common.CommonPipelineReaderPropertyValidationTest;
import io.eventuate.local.unified.cdc.pipeline.polling.factory.PollingCdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.polling.properties.PollingPipelineReaderProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PollingPipelineReaderPropertyValidationTest extends CommonPipelineReaderPropertyValidationTest {

  @Test
  public void testPollingProperties() throws Exception {
    PropertyBuilder propertyBuilder = new PropertyBuilder();

    assertExceptionMessage(propertyBuilder.toString(), PollingPipelineReaderProperties.class, "type must not be null");

    propertyBuilder.addString("type", PollingCdcPipelineReaderFactory.TYPE);
    testCommonRequiredProperties(PollingPipelineReaderProperties.class, propertyBuilder);

    assertNoException(propertyBuilder.toString(), PollingPipelineReaderProperties.class);

    PollingPipelineReaderProperties pollingPipelineReaderProperties =
            objectMapper.readValue(propertyBuilder.toString(), PollingPipelineReaderProperties.class);

    Assertions.assertEquals(500, (int)pollingPipelineReaderProperties.getPollingIntervalInMilliseconds());
    Assertions.assertEquals(1000, (int)pollingPipelineReaderProperties.getMaxEventsPerPolling());
    Assertions.assertEquals(100, (int)pollingPipelineReaderProperties.getMaxAttemptsForPolling());
    Assertions.assertEquals(500, (int)pollingPipelineReaderProperties.getPollingRetryIntervalInMilliseconds());
  }
}
