package io.eventuate.local.mysql.binlog;

import io.eventuate.common.jdbc.SchemaAndTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnOrderExtractor {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private DataSource dataSource;

  public ColumnOrderExtractor(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public Map<String, Integer> extractColumnOrders(SchemaAndTable schemaAndTable) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      try (ResultSet columnResultSet = metaData.getColumns(schemaAndTable.getSchema(),
              schemaAndTable.getSchema(),
              schemaAndTable.getTableName(),
              null)) {

        Map<String, Integer> order = new HashMap<>();

        while (columnResultSet.next()) {

          order.put(columnResultSet.getString("COLUMN_NAME").toLowerCase(),
                  columnResultSet.getInt("ORDINAL_POSITION"));
        }

        logger.info(String.format("Table %s has these columns %s", schemaAndTable, order));

        return order;
      }
    }
  }
}
