package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.sqldialect.EventuateSqlDialect;
import io.eventuate.common.jdbc.sqldialect.SqlDialectSelector;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;
import io.eventuate.local.common.MessageCleaner;
import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.common.factory.DataSourceFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.MessageCleanerProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;
import org.springframework.beans.factory.annotation.Autowired;

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

  @Autowired
  private CdcPipelineProperties defaultCdcPipelineProperties;

  @Autowired
  private CdcPipelineReaderProperties defaultCdcPipelineReaderProperties;

  private List<MessageCleaner> messageCleaners = new ArrayList<>();

  public void startMessageCleaners(Map<String, CdcPipelineProperties> cdcPipelineProperties,
                                   Map<String, CdcPipelineReaderProperties> cdcPipelineReaderProperties) {
    rawUnifiedCdcProperties.getCleaner().forEach((cleaner, rawProperties) -> {
      propertyReader.checkForUnknownProperties(rawProperties, MessageCleanerProperties.class);

      MessageCleanerProperties messageCleanerProperties =
              propertyReader.convertMapToPropertyClass(rawProperties, MessageCleanerProperties.class);

      messageCleanerProperties.validate();

      createAndStartMessageCleaner(messageCleanerProperties,
              createConnectionInfo(messageCleanerProperties, cdcPipelineProperties, cdcPipelineReaderProperties));
    });
  }

  public void stopMessageCleaners() {
    messageCleaners.forEach(MessageCleaner::stop);
  }

  private void createAndStartMessageCleaner(MessageCleanerProperties messageCleanerProperties, ConnectionInfo connectionInfo) {

    MessageCleaner messageCleaner = new MessageCleaner(connectionInfo.getEventuateSqlDialect(),
            connectionInfo.getDataSource(),
            connectionInfo.getEventuateSchema(),
            messageCleanerProperties.getClean());

    messageCleaner.start();

    messageCleaners.add(messageCleaner);
  }

  private ConnectionInfo createConnectionInfo(MessageCleanerProperties messageCleanerProperties,
                                              Map<String, CdcPipelineProperties> cdcPipelineProperties,
                                              Map<String, CdcPipelineReaderProperties> cdcPipelineReaderProperties) {
    if (messageCleanerProperties.getPipeline() != null) {
      if (messageCleanerProperties.getPipeline().toLowerCase().equals("default")) {
        return createDefaultPipelineCleanerConnectionInfo();
      } else {
        return createPipelineCleanerConnectionInfo(messageCleanerProperties, cdcPipelineProperties.get(messageCleanerProperties.getPipeline().toLowerCase()), cdcPipelineReaderProperties);
      }
    } else {
      return createCustomCleanerConnectionInfo(messageCleanerProperties);
    }
  }

  private ConnectionInfo createDefaultPipelineCleanerConnectionInfo() {
    DataSource dataSource = DataSourceFactory.createDataSource(defaultCdcPipelineReaderProperties.getDataSourceUrl(),
            defaultCdcPipelineReaderProperties.getDataSourceDriverClassName(),
            defaultCdcPipelineReaderProperties.getDataSourceUserName(),
            defaultCdcPipelineReaderProperties.getDataSourcePassword(),
            connectionPoolConfigurationProperties);

    EventuateSchema eventuateSchema = createEventuateSchema(defaultCdcPipelineProperties.getEventuateDatabaseSchema());
    EventuateSqlDialect sqlDialect = sqlDialectSelector.getDialect(defaultCdcPipelineReaderProperties.getDataSourceDriverClassName());

    return new ConnectionInfo(dataSource, eventuateSchema, sqlDialect);
  }

  private ConnectionInfo createPipelineCleanerConnectionInfo(MessageCleanerProperties messageCleanerProperties,
                                                             CdcPipelineProperties pipelineProperties,
                                                             Map<String, CdcPipelineReaderProperties> cdcPipelineReaderProperties) {
    if (pipelineProperties == null) {
      throw new RuntimeException(String.format("Cannot start cleaner pipeline %s is not found.",
              messageCleanerProperties.getPipeline()));
    }

    String reader = pipelineProperties.getReader().toLowerCase();
    CdcPipelineReaderProperties readerProperties = cdcPipelineReaderProperties.get(reader);

    DataSource dataSource = DataSourceFactory.createDataSource(readerProperties.getDataSourceUrl(),
            readerProperties.getDataSourceDriverClassName(),
            readerProperties.getDataSourceUserName(),
            readerProperties.getDataSourcePassword(),
            connectionPoolConfigurationProperties);

    EventuateSchema eventuateSchema = createEventuateSchema(pipelineProperties.getEventuateDatabaseSchema());
    EventuateSqlDialect sqlDialect = sqlDialectSelector.getDialect(readerProperties.getDataSourceDriverClassName());

    return new ConnectionInfo(dataSource, eventuateSchema, sqlDialect);
  }

  private ConnectionInfo createCustomCleanerConnectionInfo(MessageCleanerProperties messageCleanerProperties) {
    DataSource dataSource = DataSourceFactory.createDataSource(messageCleanerProperties.getDataSourceUrl(),
            messageCleanerProperties.getDataSourceDriverClassName(),
            messageCleanerProperties.getDataSourceUserName(),
            messageCleanerProperties.getDataSourcePassword(),
            connectionPoolConfigurationProperties);

    EventuateSchema eventuateSchema = createEventuateSchema(messageCleanerProperties.getEventuateSchema());
    EventuateSqlDialect sqlDialect = sqlDialectSelector.getDialect(messageCleanerProperties.getDataSourceDriverClassName());

    return new ConnectionInfo(dataSource, eventuateSchema, sqlDialect);
  }

  private EventuateSchema createEventuateSchema(String schema) {
    return new EventuateSchema(schema == null ? EventuateSchema.DEFAULT_SCHEMA : schema);
  }
}

class ConnectionInfo {
  private DataSource dataSource;
  private EventuateSchema eventuateSchema;
  private EventuateSqlDialect eventuateSqlDialect;

  public ConnectionInfo(DataSource dataSource, EventuateSchema eventuateSchema, EventuateSqlDialect eventuateSqlDialect) {
    this.dataSource = dataSource;
    this.eventuateSchema = eventuateSchema;
    this.eventuateSqlDialect = eventuateSqlDialect;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public EventuateSchema getEventuateSchema() {
    return eventuateSchema;
  }

  public EventuateSqlDialect getEventuateSqlDialect() {
    return eventuateSqlDialect;
  }
}