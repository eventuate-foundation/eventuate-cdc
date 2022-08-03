package io.eventuate.local.unified.cdc.pipeline.common.factory;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.micrometer.core.instrument.MeterRegistry;

import javax.sql.DataSource;

abstract public class CommonCdcPipelineReaderFactory<PROPERTIES extends CdcPipelineReaderProperties, READER extends BinlogEntryReader>
        implements CdcPipelineReaderFactory<PROPERTIES, READER> {

  protected MeterRegistry meterRegistry;
  protected ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties;


  public CommonCdcPipelineReaderFactory(MeterRegistry meterRegistry,
                                        ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties) {
    this.meterRegistry = meterRegistry;
    this.connectionPoolConfigurationProperties = connectionPoolConfigurationProperties;
  }

  public abstract READER create(PROPERTIES cdcPipelineReaderProperties);

  protected DataSource createDataSource(PROPERTIES properties) {
    return DataSourceFactory.createDataSource(properties.getDataSourceUrl(),
            properties.getDataSourceDriverClassName(),
            properties.getDataSourceUserName(),
            properties.getDataSourcePassword(),
            connectionPoolConfigurationProperties);
  }
}
