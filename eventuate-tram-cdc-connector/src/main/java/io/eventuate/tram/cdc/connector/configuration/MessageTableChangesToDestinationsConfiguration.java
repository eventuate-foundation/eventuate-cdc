package io.eventuate.tram.cdc.connector.configuration;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.common.spring.jdbc.sqldialect.SqlDialectConfiguration;
import io.eventuate.local.mysql.binlog.DebeziumBinlogOffsetKafkaStore;
import io.eventuate.local.unified.cdc.pipeline.dblog.common.factory.OffsetStoreFactory;
import io.eventuate.local.unified.cdc.pipeline.dblog.mysqlbinlog.factory.DebeziumOffsetStoreFactory;
import io.eventuate.tram.cdc.connector.EventuateTramChannelProperties;
import io.eventuate.tram.cdc.connector.JdbcOffsetStore;
import io.eventuate.tram.cdc.connector.configuration.condition.ActiveMQOrRabbitMQOrRedisCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

@Configuration
@Import({SqlDialectConfiguration.class,
        CommonMessageTableChangesToDestinationsConfiguration.class,
        ZookeeperConfiguration.class,
        KafkaLeadershipConfiguration.class,
        KafkaMessageTableChangesToDestinationsConfiguration.class,
        ActiveMQMessageTableChangesToDestinationsConfiguration.class,
        RabbitMQMessageTableChangesToDestinationsConfiguration.class,
        RedisMessageTableChangesToDestinationsConfiguration.class})
@EnableConfigurationProperties(EventuateTramChannelProperties.class)
public class MessageTableChangesToDestinationsConfiguration {

  @Bean
  @Conditional(ActiveMQOrRabbitMQOrRedisCondition.class)
  public DebeziumOffsetStoreFactory emptyDebeziumOffsetStoreFactory() {

    return () ->
            new DebeziumBinlogOffsetKafkaStore(null, null) {
      @Override
      public Optional<BinlogFileOffset> getLastBinlogFileOffset() {
        return Optional.empty();
      }
    };
  }

  @Bean
  @Conditional(ActiveMQOrRabbitMQOrRedisCondition.class)
  @DependsOnDatabaseInitialization
  public OffsetStoreFactory postgresWalJdbcOffsetStoreFactory() {

    return (roperties, dataSource, eventuateSchema, clientName) ->
            new JdbcOffsetStore(clientName, new JdbcTemplate(dataSource), eventuateSchema);

  }
}
