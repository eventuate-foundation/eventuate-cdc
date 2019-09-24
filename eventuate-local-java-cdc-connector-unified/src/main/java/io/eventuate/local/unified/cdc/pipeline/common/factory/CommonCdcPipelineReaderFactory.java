package io.eventuate.local.unified.cdc.pipeline.common.factory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.micrometer.core.instrument.MeterRegistry;

import javax.sql.DataSource;
import java.util.Properties;

abstract public class CommonCdcPipelineReaderFactory<PROPERTIES extends CdcPipelineReaderProperties, READER extends BinlogEntryReader>
        implements CdcPipelineReaderFactory<PROPERTIES, READER> {

  protected MeterRegistry meterRegistry;
  protected LeaderSelectorFactory leaderSelectorFactory;
  protected BinlogEntryReaderProvider binlogEntryReaderProvider;
  protected ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties;


  public CommonCdcPipelineReaderFactory(MeterRegistry meterRegistry,
                                        LeaderSelectorFactory leaderSelectorFactory,
                                        BinlogEntryReaderProvider binlogEntryReaderProvider,
                                        ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties) {
    this.meterRegistry = meterRegistry;
    this.leaderSelectorFactory = leaderSelectorFactory;
    this.binlogEntryReaderProvider = binlogEntryReaderProvider;
    this.connectionPoolConfigurationProperties = connectionPoolConfigurationProperties;
  }

  public abstract READER create(PROPERTIES cdcPipelineReaderProperties);

  protected DataSource createDataSource(PROPERTIES properties) {
    Properties config = new Properties();

    config.setProperty("jdbcUrl", properties.getDataSourceUrl());
    config.setProperty("driverClassName", properties.getDataSourceDriverClassName());
    config.setProperty("username", properties.getDataSourceUserName());
    config.setProperty("password", properties.getDataSourcePassword());
    config.setProperty("initializationFailTimeout", String.valueOf(Long.MAX_VALUE));
    config.setProperty("connectionTestQuery", "select 1");

    connectionPoolConfigurationProperties.getProperties().forEach(config::setProperty);

    HikariDataSource hikariDataSource = new HikariDataSource(new HikariConfig(config));

    return hikariDataSource;
  }
}
