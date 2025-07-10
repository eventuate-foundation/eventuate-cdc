package io.eventuate.local.mysql.binlog;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.common.jdbc.SchemaAndTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

public abstract class AbstractColumnOrderExtractorTest {

  @Value("${eventuate.database.schema:#{null}}")
  private String schema;

  @Autowired
  protected DataSource dataSource;

  @Test
  public void testExtractor() throws SQLException  {
    ColumnOrderExtractor columnOrderExtractor = new ColumnOrderExtractor(dataSource);

    Map<String, Integer> columnOrders = columnOrderExtractor
            .extractColumnOrders(new SchemaAndTable(schema == null ? EventuateSchema.DEFAULT_SCHEMA : schema,
                    "cdc_monitoring"));

    Assertions.assertFalse(columnOrders.isEmpty());
    Assertions.assertTrue(columnOrders.containsKey("last_time"));
    Assertions.assertTrue(columnOrders.containsKey("reader_id"));
  }
}
