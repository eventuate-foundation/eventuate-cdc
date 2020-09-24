package io.eventuate.local.mysql.binlog;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.local.test.util.TestHelper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.util.Optional;

@Configuration
@EnableAutoConfiguration
@Import({SqlDialectConfiguration.class, OffsetStoreMockConfiguration.class})
public class MySqlBinlogEntryReaderMessageTableTestConfiguration {

  @Bean
  public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
    return new EventuateSchema(eventuateDatabaseSchema);
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public MySqlBinaryLogClient mySqlBinaryLogClient(@Autowired MeterRegistry meterRegistry,
                                                   @Value("${spring.datasource.url}") String dataSourceURL,
                                                   DataSource dataSource,
                                                   EventuateConfigurationProperties eventuateConfigurationProperties,
                                                   OffsetStore offsetStore) {

    return new MySqlBinaryLogClient(
            meterRegistry,
            eventuateConfigurationProperties.getDbUserName(),
            eventuateConfigurationProperties.getDbPassword(),
            dataSourceURL,
            dataSource,
            eventuateConfigurationProperties.getReaderName(),
            eventuateConfigurationProperties.getMySqlBinlogClientUniqueId(),
            eventuateConfigurationProperties.getBinlogConnectionTimeoutInMilliseconds(),
            eventuateConfigurationProperties.getMaxAttemptsForBinlogConnection(),
            offsetStore,
            Optional.empty(),
            eventuateConfigurationProperties.getReplicationLagMeasuringIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryAttempts(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
            eventuateConfigurationProperties.getReaderId());
  }

  @Bean
  public TestHelper testHelper() {
    return new TestHelper();
  }
}
