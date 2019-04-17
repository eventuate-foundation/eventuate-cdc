package io.eventuate.local.common;

import io.eventuate.common.BinlogFileOffset;

public interface BinlogEntry {
  Object getColumn(String name);
  BinlogFileOffset getBinlogFileOffset();
}
