package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;

import java.nio.charset.StandardCharsets;

public interface BinlogEntry {
  Object getColumn(String name);
  BinlogFileOffset getBinlogFileOffset();

  default String getStringColumn(String name) {
    Object columnValue = getColumn(name);

    if (columnValue == null) {
      return null;
    }

    if (columnValue instanceof String) return (String) columnValue;

    if (!(columnValue instanceof byte[])) throw new IllegalArgumentException(String.format("Unexpected type %s of column %s", columnValue.getClass(), name));

    return new String((byte[]) columnValue, StandardCharsets.UTF_8);
  }
}
