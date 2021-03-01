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

  private Boolean purgeMessagesEnabled = false;
  private int purgeMessagesMaxAgeInSeconds = (int) TimeUnit.DAYS.toSeconds(2);
  private Boolean purgeReceivedMessagesEnabled = false;
  private int purgeReceivedMessagesMaxAgeInSeconds = (int)TimeUnit.DAYS.toSeconds(2);

  private int purgeIntervalInSeconds = (int)TimeUnit.MINUTES.toSeconds(1);

  @Override
  public void validate() {
    Assert.notNull(dataSourceUrl, "dataSourceUrl must not be null");
    Assert.notNull(dataSourceUserName, "dataSourceUserName must not be null");
    Assert.notNull(dataSourcePassword, "dataSourcePassword must not be null");
    Assert.notNull(dataSourceDriverClassName, "dataSourceDriverClassName must not be null");
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

  public void setEventuateSchema(String eventuateSchema) {
    this.eventuateSchema = eventuateSchema;
  }

  public Boolean getPurgeMessagesEnabled() {
    return purgeMessagesEnabled;
  }

  public void setPurgeMessagesEnabled(Boolean purgeMessagesEnabled) {
    this.purgeMessagesEnabled = purgeMessagesEnabled;
  }

  public int getPurgeMessagesMaxAgeInSeconds() {
    return purgeMessagesMaxAgeInSeconds;
  }

  public void setPurgeMessagesMaxAgeInSeconds(int purgeMessagesMaxAgeInSeconds) {
    this.purgeMessagesMaxAgeInSeconds = purgeMessagesMaxAgeInSeconds;
  }

  public Boolean getPurgeReceivedMessagesEnabled() {
    return purgeReceivedMessagesEnabled;
  }

  public void setPurgeReceivedMessagesEnabled(Boolean purgeReceivedMessagesEnabled) {
    this.purgeReceivedMessagesEnabled = purgeReceivedMessagesEnabled;
  }

  public int getPurgeReceivedMessagesMaxAgeInSeconds() {
    return purgeReceivedMessagesMaxAgeInSeconds;
  }

  public void setPurgeReceivedMessagesMaxAgeInSeconds(int purgeReceivedMessagesMaxAgeInSeconds) {
    this.purgeReceivedMessagesMaxAgeInSeconds = purgeReceivedMessagesMaxAgeInSeconds;
  }

  public int getPurgeIntervalInSeconds() {
    return purgeIntervalInSeconds;
  }

  public void setPurgeIntervalInSeconds(int purgeIntervalInSeconds) {
    this.purgeIntervalInSeconds = purgeIntervalInSeconds;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
