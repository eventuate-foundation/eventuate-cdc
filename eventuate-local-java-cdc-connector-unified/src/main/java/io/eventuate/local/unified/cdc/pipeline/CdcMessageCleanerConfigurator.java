package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.common.MessageCleaner;
import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.common.factory.DataSourceFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CdcMessageCleanerConfigurator {

  private PropertyReader propertyReader = new PropertyReader();

  @Autowired
  private RawUnifiedCdcProperties rawUnifiedCdcProperties;

  @Autowired
  private ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties;

  @Autowired
  private SqlDialectSelector sqlDialectSelector;

  private List<MessageCleaner> messageCleaners = new ArrayList<>();

  @PostConstruct
  public void startMessageCleaners() {
    rawUnifiedCdcProperties.getCleaner().forEach((cleaner, rawProperties) -> createMessageCleaner(rawProperties));
  }

  @PreDestroy
  public void stopMessageCleaners() {
    messageCleaners.forEach(MessageCleaner::stop);
  }

  private void createMessageCleaner(Map<String, Object> rawProperties) {
    propertyReader.checkForUnknownProperties(rawProperties, MessageCleanerProperties.class);

    MessageCleanerProperties messageCleanerProperties =
            propertyReader.convertMapToPropertyClass(rawProperties, MessageCleanerProperties.class);

    MessageCleaner messageCleaner = new MessageCleaner(sqlDialectSelector.getDialect(messageCleanerProperties.getDataSourceDriverClassName()),
            createDataSource(messageCleanerProperties),
            createEventuateSchema(messageCleanerProperties),
            messageCleanerProperties.getPurgeMessagesEnabled(),
            messageCleanerProperties.getPurgeMessagesMaxAgeInSeconds(),
            messageCleanerProperties.getPurgeReceivedMessagesEnabled(),
            messageCleanerProperties.getPurgeReceivedMessagesMaxAgeInSeconds(),
            messageCleanerProperties.getPurgeIntervalInSeconds());

    messageCleaner.start();

    messageCleaners.add(messageCleaner);
  }

  private DataSource createDataSource(MessageCleanerProperties messageCleanerProperties) {
    return DataSourceFactory.createDataSource(messageCleanerProperties.getDataSourceUrl(),
            messageCleanerProperties.getDataSourceDriverClassName(),
            messageCleanerProperties.getDataSourceUserName(),
            messageCleanerProperties.getDataSourcePassword(),
            connectionPoolConfigurationProperties);
  }

  private EventuateSchema createEventuateSchema(MessageCleanerProperties messageCleanerProperties) {
    String schema = messageCleanerProperties.getEventuateSchema() == null ? EventuateSchema.EMPTY_SCHEMA : messageCleanerProperties.getEventuateSchema();

    return new EventuateSchema(schema);
  }
}
