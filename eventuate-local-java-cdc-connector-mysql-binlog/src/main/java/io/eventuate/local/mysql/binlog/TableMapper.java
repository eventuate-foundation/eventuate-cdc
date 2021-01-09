package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import io.eventuate.common.jdbc.SchemaAndTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableMapper {
  private final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();
  private final Map<SchemaAndTable, Long> tableIdBySchemaAndTable = new HashMap<>();

  public Map<Long, TableMapEventData> getMappings() {
    return Collections.unmodifiableMap(tableMapEventByTableId);
  }

  public SchemaAndTable getSchemaAndTable(long tableId) {
    TableMapEventData tableMapEventData = tableMapEventByTableId.get(tableId);

    return new SchemaAndTable(tableMapEventData.getDatabase(), tableMapEventData.getTable());
  }

  public boolean addMappingAndCheckIfColumnRefreshIsNecessary(TableMapEventData tableMapEventData) {
    boolean refresh = shouldRefreshColumns(tableMapEventData);

    addMapping(tableMapEventData);

    return refresh;
  }

  public void addMapping(TableMapEventData tableMapEventData) {
    SchemaAndTable schemaAndTable = new SchemaAndTable(tableMapEventData.getDatabase(), tableMapEventData.getTable());
    Long previousTableIdOfSchemaAndTable = tableIdBySchemaAndTable.put(schemaAndTable, tableMapEventData.getTableId());
    clearPreviousMapping(previousTableIdOfSchemaAndTable, schemaAndTable);

    tableMapEventByTableId.put(tableMapEventData.getTableId(), tableMapEventData);
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

  private void clearPreviousMapping(Long previousTableIdOfCurrentSchemaAndTable, SchemaAndTable currentSchemaAndTable) {
    if (previousTableIdOfCurrentSchemaAndTable == null) {
      return;
    }

    TableMapEventData previousTableMapEventDataOfSchemaAndTable = tableMapEventByTableId.get(previousTableIdOfCurrentSchemaAndTable);

    if (previousTableMapEventDataOfSchemaAndTable == null) {
      return;
    }

    SchemaAndTable previousSchemaAndTable = new SchemaAndTable(previousTableMapEventDataOfSchemaAndTable.getDatabase(),
            previousTableMapEventDataOfSchemaAndTable.getTable());

    if (previousSchemaAndTable.equals(currentSchemaAndTable)) {
      tableMapEventByTableId.remove(previousTableIdOfCurrentSchemaAndTable);
    }
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
