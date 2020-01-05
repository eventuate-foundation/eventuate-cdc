package io.eventuate.local.db.log.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.common.GenericOffsetStore;

import java.util.Optional;

public interface OffsetStore extends GenericOffsetStore<BinlogFileOffset> {
  Optional<BinlogFileOffset> getLastBinlogFileOffset();
  void save(BinlogFileOffset binlogFileOffset);
}
