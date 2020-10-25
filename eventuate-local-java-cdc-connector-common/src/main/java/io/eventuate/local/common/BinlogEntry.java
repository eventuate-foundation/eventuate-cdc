package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;

public interface BinlogEntry {
  Object getColumn(String name);
  BinlogFileOffset getBinlogFileOffset();

  default String getJsonColumn(String name) {
    return getStringColumn(name);
  }

  default boolean getBooleanColumn(String name) {
    Object columnValue = getColumn(name);

    if (columnValue instanceof Number) return ((Number) getColumn(name)).intValue() != 0; //Integer - mysql, Short - mssql
    if (columnValue instanceof String) return Integer.parseInt((String)getColumn(name)) != 0; // String - postgres

    throw new IllegalArgumentException(String.format("Unexpected type %s of column %s, should be int or stringified int",
            columnValue.getClass(), name));
  }

  default Long getLongColumn(String name) {
    Object columnValue = getColumn(name);

    if (columnValue == null) {
      return null;
    }

    if (columnValue instanceof Long) return (Long)columnValue; //mysql
    if (columnValue instanceof String) return Long.parseLong((String)columnValue); //postgres

    throw new IllegalArgumentException(String.format("Unexpected type %s of column %s, should be bigint or stringified bigint",
            columnValue.getClass(), name));
  }

  default String getStringColumn(String name) {
    Object columnValue = getColumn(name);

    if (columnValue == null) {
      return null;
    }

    if (columnValue instanceof String) return (String) columnValue;

    throw new IllegalArgumentException(String.format("Unexpected type %s of column %s, should be String",
            columnValue.getClass(), name));
  }
}
