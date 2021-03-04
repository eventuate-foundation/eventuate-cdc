package io.eventuate.local.unified.cdc.pipeline.common.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.Map;

@ConfigurationProperties(prefix = "eventuate.cdc")
public class RawUnifiedCdcProperties {
  private Map<String, Map<String, Object>> reader;

  private Map<String, Map<String, Object>> pipeline;

  private Map<String, Map<String, Object>> cleaner = Collections.emptyMap();

  public Map<String, Map<String, Object>> getReader() {
    return reader;
  }

  public void setReader(Map<String, Map<String, Object>> reader) {
    this.reader = reader;
  }

  public Map<String, Map<String, Object>> getPipeline() {
    return pipeline;
  }

  public void setPipeline(Map<String, Map<String, Object>> pipeline) {
    this.pipeline = pipeline;
  }

  public Map<String, Map<String, Object>> getCleaner() {
    return cleaner;
  }

  public void setCleaner(Map<String, Map<String, Object>> cleaner) {
    this.cleaner = cleaner;
  }

  public boolean isReaderPropertiesDeclared() {
    return reader != null && !reader.isEmpty();
  }

  public boolean isPipelinePropertiesDeclared() {
    return pipeline != null && !pipeline.isEmpty();
  }
}
