package io.eventuate.local.unified.cdc.pipeline.common.properties;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.eventuate.local.common.MessageCleaningProperties;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.springframework.util.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageCleanerProperties implements ValidatableProperties {
  private String dataSourceUrl;
  private String dataSourceUserName;
  private String dataSourcePassword;
  private String dataSourceDriverClassName;
  private String eventuateSchema;

  private String pipeline;

  private MessageCleaningProperties clean = new MessageCleaningProperties();

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

  public MessageCleaningProperties getClean() {
    return clean;
  }

  public void setClean(MessageCleaningProperties clean) {
    this.clean = clean;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
