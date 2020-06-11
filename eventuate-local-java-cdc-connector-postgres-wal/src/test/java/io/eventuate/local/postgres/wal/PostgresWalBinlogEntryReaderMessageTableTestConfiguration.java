package io.eventuate.local.postgres.wal;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.test.util.TestHelper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;

@Configuration
@EnableAutoConfiguration
@Import(SqlDialectConfiguration.class)
public class PostgresWalBinlogEntryReaderMessageTableTestConfiguration {


  @Bean
  public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
    return new EventuateSchema(eventuateDatabaseSchema);
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public PostgresWalClient postgresWalClient(MeterRegistry meterRegistry,
                                             @Value("${spring.datasource.url}") String dbUrl,
                                             @Value("${spring.datasource.username}") String dbUserName,
                                             @Value("${spring.datasource.password}") String dbPassword,
                                             DataSource dataSource,
                                             EventuateConfigurationProperties eventuateConfigurationProperties) {

    return new PostgresWalClient(meterRegistry,
            dbUrl,
            dbUserName,
            dbPassword,
            eventuateConfigurationProperties.getPostgresWalIntervalInMilliseconds(),
            eventuateConfigurationProperties.getBinlogConnectionTimeoutInMilliseconds(),
            eventuateConfigurationProperties.getMaxAttemptsForBinlogConnection(),
            eventuateConfigurationProperties.getPostgresReplicationStatusIntervalInMilliseconds(),
            eventuateConfigurationProperties.getPostgresReplicationSlotName(),
            dataSource,
            eventuateConfigurationProperties.getReaderName(),
            eventuateConfigurationProperties.getReplicationLagMeasuringIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getMonitoringRetryAttempts(),
            eventuateConfigurationProperties.getAdditionalServiceReplicationSlotName(),
            eventuateConfigurationProperties.getWaitForOffsetSyncTimeoutInMilliseconds(),
            new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA));
  }

  @Bean
  public TestHelper testHelper() {
    return new TestHelper();
  }
}
