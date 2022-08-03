package io.eventuate.local.unified.cdc.pipeline.polling;

import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.polling.properties.PollingPipelineReaderProperties;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class PollingPipelineReaderPropertiesConversionTest {

    @Test
    public void shouldConvertProperties() {
        PropertyReader propertyReader = new PropertyReader();
        Map<String, Object> properties = Collections.singletonMap("pollingParallelChannelNames", "x,y");
        PollingPipelineReaderProperties readerProps = propertyReader.convertMapToPropertyClass(properties, PollingPipelineReaderProperties.class);
        Set<String> expectedNames = new HashSet<>();
        expectedNames.add("x");
        expectedNames.add("y");
        assertEquals(expectedNames, readerProps.getPollingParallelChannels());
    }
}
