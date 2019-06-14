package io.eventuate.local.cdc.debezium.migration;

import io.eventuate.common.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.messaging.kafka.common.EventuateKafkaPropertiesConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableAutoConfiguration
@Import({EventuateKafkaPropertiesConfiguration.class, SqlDialectConfiguration.class})
public class MigrationE2ETestConfiguration {
}
