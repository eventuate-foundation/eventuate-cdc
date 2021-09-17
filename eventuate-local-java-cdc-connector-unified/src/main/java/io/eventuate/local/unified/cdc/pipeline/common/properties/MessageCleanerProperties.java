package io.eventuate.local.unified.cdc.pipeline.common.properties;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageCleanerProperties implements ValidatableProperties {
  private String dataSourceUrl;
  private String dataSourceUserName;
  private String dataSourcePassword;
  private String dataSourceDriverClassName;
  private String eventuateSchema;

  private String pipeline;

  private boolean messageCleaningEnabled = false;
  private int messagesMaxAgeInSeconds = (int) TimeUnit.DAYS.toSeconds(2);
  private boolean receivedMessageCleaningEnabled = false;
  private int receivedMessagesMaxAgeInSeconds = (int)TimeUnit.DAYS.toSeconds(2);
  private int intervalInSeconds = (int)TimeUnit.MINUTES.toSeconds(1);

  @Override
  public void validate() {
    if (pipeline == null) {
      Assert.notNull(dataSourceUrl, "dataSourceUrl must not be null if pipeline is not specified");
      Assert.notNull(dataSourceUserName, "dataSourceUserName must not be null if pipeline is not specified");
      Assert.notNull(dataSourcePassword, "dataSourcePassword must not be null if pipeline is not specified");
      Assert.notNull(dataSourceDriverClassName, "dataSourceDriverClassName must not be null if pipeline is not specified");
    }
  }


  public String getDataSourceUrl() {
    return dataSourceUrl;
  }

  public void setDataSourceUrl(String dataSourceUrl) {
    this.dataSourceUrl = dataSourceUrl;
  }

  public String getDataSourceUserName() {
    return dataSourceUserName;
  }

  public void setDataSourceUserName(String dataSourceUserName) {
    this.dataSourceUserName = dataSourceUserName;
  }

  public String getDataSourcePassword() {
    return dataSourcePassword;
  }

  public void setDataSourcePassword(String dataSourcePassword) {
    this.dataSourcePassword = dataSourcePassword;
  }

  public String getDataSourceDriverClassName() {
    return dataSourceDriverClassName;
  }

  public void setDataSourceDriverClassName(String dataSourceDriverClassName) {
    this.dataSourceDriverClassName = dataSourceDriverClassName;
  }

  public String getEventuateSchema() {
    return eventuateSchema;
  }

  public String getPipeline() {
    return pipeline;
  }

  public void setPipeline(String pipeline) {
    this.pipeline = pipeline;
  }

  public void setEventuateSchema(String eventuateSchema) {
    this.eventuateSchema = eventuateSchema;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

  public boolean isMessageCleaningEnabled() {
    return messageCleaningEnabled;
  }

  public void setMessageCleaningEnabled(boolean messageCleaningEnabled) {
    this.messageCleaningEnabled = messageCleaningEnabled;
  }

  public int getMessagesMaxAgeInSeconds() {
    return messagesMaxAgeInSeconds;
  }

  public void setMessagesMaxAgeInSeconds(int messagesMaxAgeInSeconds) {
    this.messagesMaxAgeInSeconds = messagesMaxAgeInSeconds;
  }

  public boolean isReceivedMessageCleaningEnabled() {
    return receivedMessageCleaningEnabled;
  }

  public void setReceivedMessageCleaningEnabled(boolean receivedMessageCleaningEnabled) {
    this.receivedMessageCleaningEnabled = receivedMessageCleaningEnabled;
  }

  public int getReceivedMessagesMaxAgeInSeconds() {
    return receivedMessagesMaxAgeInSeconds;
  }

  public void setReceivedMessagesMaxAgeInSeconds(int receivedMessagesMaxAgeInSeconds) {
    this.receivedMessagesMaxAgeInSeconds = receivedMessagesMaxAgeInSeconds;
  }

  public int getIntervalInSeconds() {
    return intervalInSeconds;
  }

  public void setIntervalInSeconds(int intervalInSeconds) {
    this.intervalInSeconds = intervalInSeconds;
  }
}
