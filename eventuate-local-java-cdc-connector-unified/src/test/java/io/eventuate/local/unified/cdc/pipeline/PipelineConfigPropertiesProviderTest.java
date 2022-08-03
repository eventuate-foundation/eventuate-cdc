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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;


@RunWith(SpringJUnit4ClassRunner.class)
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
        List<CdcPipelineReaderProperties> readers = pipelineConfigPropertiesProvider.pipelineReaderProperties().get();
        assertEquals(1, readers.size());
        PollingPipelineReaderProperties reader = (PollingPipelineReaderProperties) readers.get(0);
        Set<String> expectedChannels = new HashSet<>();
        expectedChannels.add("parallel_channel_1");
        expectedChannels.add("parallel_channel_2");
        assertEquals(expectedChannels, reader.getPollingParallelChannels());
    }

    @Test
    public void shouldProvidePipelineProperties() {
        List<CdcPipelineProperties> pipelines = pipelineConfigPropertiesProvider.pipelineProperties().get();
        assertEquals(2, pipelines.size());
    }
}