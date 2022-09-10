package io.eventuate.local.unified.cdc.pipeline.polling;

import io.eventuate.common.jdbc.OutboxTableSuffix;
import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.polling.properties.PollingPipelineReaderProperties;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    @Test
    public void shouldConvertPropertiesForOutboxPartitioning() {
        PropertyReader propertyReader = new PropertyReader();
        Map<String, Object> properties = Collections.singletonMap("outboxTables", "8");
        PollingPipelineReaderProperties readerProps = propertyReader.convertMapToPropertyClass(properties, PollingPipelineReaderProperties.class);
        List<OutboxTableSuffix> suffixes = IntStream.range(0, 8).mapToObj(OutboxTableSuffix::new).collect(Collectors.toList());
        assertEquals(suffixes, readerProps.getOutboxPartitioning().outboxTableSuffixes());
    }
}
