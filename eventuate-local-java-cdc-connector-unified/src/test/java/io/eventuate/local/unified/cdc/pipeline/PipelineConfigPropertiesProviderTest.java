package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import io.eventuate.local.unified.cdc.pipeline.polling.factory.PollingCdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.polling.properties.PollingPipelineReaderProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest(classes = PipelineConfigPropertiesProviderTest.Config.class, properties = "eventuatelocal.cdc.polling.parallel.channels=x,y")
@TestPropertySource(locations="/sample-pipeline-config.properties")
public class PipelineConfigPropertiesProviderTest {

    @Configuration
    @EnableConfigurationProperties(RawUnifiedCdcProperties.class)
    public static class Config {

        @Bean
        public PipelineConfigPropertiesProvider pipelineConfigPropertiesProvider(RawUnifiedCdcProperties rawUnifiedCdcProperties, Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories) {
            return new PipelineConfigPropertiesProvider(rawUnifiedCdcProperties, cdcPipelineReaderFactories);
        }

        @Bean
        public PollingCdcPipelineReaderFactory pollingCdcPipelineReaderFactory(ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties, SqlDialectSelector sqlDialectSelector, MeterRegistry meterRegistry) {
            return new PollingCdcPipelineReaderFactory(meterRegistry, sqlDialectSelector, connectionPoolConfigurationProperties);
        }
    }

    @MockBean
    private MeterRegistry meterRegistry;

    @MockBean
    private SqlDialectSelector sqlDialectSelector;

    @MockBean
    private ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties;

    @Autowired
    private PipelineConfigPropertiesProvider pipelineConfigPropertiesProvider;

    @Test
    public void shouldProvideReaderProperties() {
        Map<String, CdcPipelineReaderProperties> readers = pipelineConfigPropertiesProvider.pipelineReaderProperties().get();
        assertEquals(2, readers.size());

        PollingPipelineReaderProperties mysqlreader1 = (PollingPipelineReaderProperties) readers.get("mysqlreader1");
        Set<String> expectedChannels = new HashSet<>();
        expectedChannels.add("parallel_channel_1");
        expectedChannels.add("parallel_channel_2");
        assertEquals(expectedChannels, mysqlreader1.getPollingParallelChannels());

        PollingPipelineReaderProperties mysqlreader2 = (PollingPipelineReaderProperties) readers.get("mysqlreader2");
        assertEquals(emptySet(), mysqlreader2.getPollingParallelChannels());

    }

    @Test
    public void shouldProvidePipelineProperties() {
        Map<String, CdcPipelineProperties> pipelines = pipelineConfigPropertiesProvider.pipelineProperties().get();
        assertEquals(2, pipelines.size());
    }
}