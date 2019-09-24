package io.eventuate.local.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "eventuate.cdc.connection")
public class ConnectionPoolConfigurationProperties {
  public static final int DEFAULT_MINIMUM_IDLE_CONNECTIONS = 1;

  private Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    properties.putIfAbsent("minimumIdle", String.valueOf(DEFAULT_MINIMUM_IDLE_CONNECTIONS));

    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }
}
