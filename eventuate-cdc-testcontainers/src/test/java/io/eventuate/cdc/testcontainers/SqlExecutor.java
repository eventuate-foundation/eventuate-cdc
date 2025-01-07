package io.eventuate.cdc.testcontainers;

import java.sql.SQLException;

interface SqlExecutor {
  void execute(String ddl) throws SQLException;
}
