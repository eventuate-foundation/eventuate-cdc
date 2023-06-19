package io.eventuate.local.postgres.wal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConnectionFactory {
    public Connection create(String dataSourceUrl, Properties props) throws SQLException {
        return DriverManager.getConnection(dataSourceUrl, props);
    }
}
