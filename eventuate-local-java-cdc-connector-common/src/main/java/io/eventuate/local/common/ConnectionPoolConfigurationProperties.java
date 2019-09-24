package io.eventuate.local.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.Map;

@ConfigurationProperties(prefix = "connection")
public class ConnectionPoolConfigurationProperties {
  private Map<String, String> pool = Collections.emptyMap();

  public Map<String, String> getPool() {
    return pool;
  }

  public void setPool(Map<String, String> pool) {
    this.pool = pool;
  }
}
