package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import io.eventuate.local.common.SchemaAndTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableMapper {
  private final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();

  public Map<Long, TableMapEventData> getMappings() {
    return Collections.unmodifiableMap(tableMapEventByTableId);
  }

  public SchemaAndTable getSchemaAndTable(long tableId) {
    TableMapEventData tableMapEventData = tableMapEventByTableId.get(tableId);

    return new SchemaAndTable(tableMapEventData.getDatabase(), tableMapEventData.getTable());
  }

  public boolean addMapping(TableMapEventData tableMapEventData) {
    boolean refresh = shouldRefreshColumns(tableMapEventData);

    tableMapEventByTableId.put(tableMapEventData.getTableId(), tableMapEventData);

    return refresh;
  }

  public boolean isTableMapped(long tableId) {
    return tableMapEventByTableId.containsKey(tableId);
  }

  public void removeMapping(TableMapEventData tableMapEventData) {
    tableMapEventByTableId.remove(tableMapEventData.getTableId());
  }

  public void clearMappings() {
    tableMapEventByTableId.clear();
  }


  private boolean shouldRefreshColumns(TableMapEventData updatedData) {
    if (tableMapEventByTableId.containsKey(updatedData.getTableId())) {
      TableMapEventData currentData = tableMapEventByTableId.get(updatedData.getTableId());
      return isTableSchemaChanged(currentData, updatedData);
    } else {
      return true;
    }
  }

  private boolean isTableSchemaChanged(TableMapEventData currentData, TableMapEventData updatedData) {
    return !(Arrays.equals(currentData.getColumnTypes(), updatedData.getColumnTypes()) &&
            Arrays.equals(currentData.getColumnMetadata(), updatedData.getColumnMetadata()));
  }
}
