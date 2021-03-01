package io.eventuate.local.unified.cdc.pipeline.common.factory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.eventuate.local.common.ConnectionPoolConfigurationProperties;

import javax.sql.DataSource;
import java.util.Properties;

public class DataSourceFactory {
  public static DataSource createDataSource(String jdbcUrl,
                                            String driverClassName,
                                            String username,
                                            String password,
                                            ConnectionPoolConfigurationProperties connectionPoolConfigurationProperties) {
    Properties config = new Properties();

    config.setProperty("jdbcUrl", jdbcUrl);
    config.setProperty("driverClassName", driverClassName);
    config.setProperty("username", username);
    config.setProperty("password", password);
    config.setProperty("initializationFailTimeout", String.valueOf(Long.MAX_VALUE));
    config.setProperty("connectionTestQuery", "select 1");

    connectionPoolConfigurationProperties.getProperties().forEach(config::setProperty);

    HikariDataSource hikariDataSource = new HikariDataSource(new HikariConfig(config));

    return hikariDataSource;
  }
}
