package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.eventuate.common.jdbc.SchemaAndTable;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMySqlBinlogExtractor {

  private Map<SchemaAndTable, Map<String, Integer>> columnOrders = new HashMap<>();
  private ColumnOrderExtractor columnOrderExtractor;
  private boolean columnOrderShouldBeRefreshed;

  public AbstractMySqlBinlogExtractor(DataSource dataSource) {
    this.columnOrderExtractor = new ColumnOrderExtractor(dataSource);
  }

  public void refreshColumnOrder() {
    columnOrderShouldBeRefreshed = true;
  }

  protected void updateColumnOrders(SchemaAndTable schemaAndTable) {
    if (!columnOrders.containsKey(schemaAndTable) || columnOrderShouldBeRefreshed) {
      columnOrderShouldBeRefreshed = false;
      try {
        columnOrders.put(schemaAndTable, columnOrderExtractor.extractColumnOrders(schemaAndTable));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected Serializable getValue(SchemaAndTable schemaAndTable, WriteRowsEventData eventData, String columnName) {
    return getValue(schemaAndTable, new EventDataAdapter(eventData), columnName);
  }

  protected Serializable getValue(SchemaAndTable schemaAndTable, UpdateRowsEventData eventData, String columnName) {
    return getValue(schemaAndTable, new EventDataAdapter(eventData), columnName);
  }

  private Serializable getValue(SchemaAndTable schemaAndTable, EventDataAdapter eventDataAdapter, String columnName) {
    if (columnOrders.containsKey(schemaAndTable)) {
      Map<String, Integer> order = columnOrders.get(schemaAndTable);

      if (order.containsKey(columnName)) {
        return eventDataAdapter.getValue(order, columnName);
      }
    }

    throw new RuntimeException("For table %s Column with name [%s] not found in %s".formatted(
            schemaAndTable, columnName, columnOrders));
  }

  private static class EventDataAdapter {
    private UpdateRowsEventData updateRowsEventData;
    private WriteRowsEventData writeRowsEventData;

    public EventDataAdapter(UpdateRowsEventData updateRowsEventData) {
      this.updateRowsEventData = updateRowsEventData;
    }

    public EventDataAdapter(WriteRowsEventData writeRowsEventData) {
      this.writeRowsEventData = writeRowsEventData;
    }

    public Serializable getValue(Map<String, Integer> order, String columnName) {
      if (writeRowsEventData != null) {
        return writeRowsEventData.getRows().get(0)[order.get(columnName) - 1];
      }

      if (updateRowsEventData != null) {
        return updateRowsEventData.getRows().get(0).getValue()[order.get(columnName) - 1];
      }

      throw new IllegalArgumentException("Event is not provided");
    }
  }

}
