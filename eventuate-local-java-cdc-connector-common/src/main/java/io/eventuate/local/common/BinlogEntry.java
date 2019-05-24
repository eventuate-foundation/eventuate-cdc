package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;

public interface BinlogEntry {
  Object getColumn(String name);
  BinlogFileOffset getBinlogFileOffset();
}
