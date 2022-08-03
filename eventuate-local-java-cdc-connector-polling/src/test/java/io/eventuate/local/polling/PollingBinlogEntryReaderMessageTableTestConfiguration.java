package io.eventuate.local.polling;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.common.spring.id.IdGeneratorConfiguration;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.common.EventuateConfigurationProperties;
import io.eventuate.local.test.util.SourceTableNameSupplier;
import io.eventuate.local.test.util.TestHelper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Configuration
@EnableAutoConfiguration
@Import({SqlDialectConfiguration.class, IdGeneratorConfiguration.class})
public class PollingBinlogEntryReaderMessageTableTestConfiguration {

  @Bean
  public EventuateSchema eventuateSchema(@Value("${eventuate.database.schema:#{null}}") String eventuateDatabaseSchema) {
    return new EventuateSchema(eventuateDatabaseSchema);
  }

  @Bean
  public EventuateConfigurationProperties eventuateConfigurationProperties() {
    return new EventuateConfigurationProperties();
  }

  @Bean
  public SourceTableNameSupplier sourceTableNameSupplier(EventuateConfigurationProperties eventuateConfigurationProperties) {
    return new SourceTableNameSupplier(eventuateConfigurationProperties.getSourceTableName() == null ? "message" : eventuateConfigurationProperties.getSourceTableName());
  }

  @Bean
  @Profile("EventuatePolling")
  public PollingDao pollingDao(@Autowired(required = false) MeterRegistry meterRegistry,
                               @Value("${spring.datasource.url}") String dataSourceURL,
                               @Value("${spring.datasource.driver-class-name}") String driver,
                               EventuateConfigurationProperties eventuateConfigurationProperties,
                               DataSource dataSource,
                               SqlDialectSelector sqlDialectSelector) {

    return new PollingDao(meterRegistry,
            dataSourceURL,
            dataSource,
            eventuateConfigurationProperties.getMaxEventsPerPolling(),
            eventuateConfigurationProperties.getMaxAttemptsForPolling(),
            eventuateConfigurationProperties.getPollingRetryIntervalInMilliseconds(),
            eventuateConfigurationProperties.getPollingIntervalInMilliseconds(),
            eventuateConfigurationProperties.getReaderName(),
            sqlDialectSelector.getDialect(driver),
            eventuateConfigurationProperties.getOutboxId(), ParallelPollingChannels.make(eventuateConfigurationProperties.getPollingParallelChannels()));
  }

  @Bean
  public TestHelper testHelper() {
    return new TestHelper();
  }
}
