package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.db.log.common.OffsetStore;

import java.util.Optional;

public class OffsetStoreMock implements OffsetStore {

  Optional<BinlogFileOffset> binlogFileOffset = Optional.empty();
  boolean throwExceptionOnSave = false;
  boolean throwExceptionOnLoad = false;

  @Override
  public synchronized Optional<BinlogFileOffset> getLastBinlogFileOffset() {
    if (throwExceptionOnLoad) {
      throw new IllegalStateException("Loading offset is disabled");
    }

    return binlogFileOffset;
  }

  @Override
  public synchronized void save(BinlogFileOffset binlogFileOffset) {
    if (throwExceptionOnSave) {
      throw new IllegalStateException("Saving offset is disabled");
    }

    this.binlogFileOffset = Optional.ofNullable(binlogFileOffset);
  }
}
