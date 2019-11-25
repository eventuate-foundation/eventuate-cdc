package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;

import java.util.concurrent.CompletableFuture;

class FutureOffset {
  private CompletableFuture<?> future;
  private BinlogFileOffset binlogFileOffset;

  public FutureOffset(CompletableFuture<?> future, BinlogFileOffset binlogFileOffset) {
    this.future = future;
    this.binlogFileOffset = binlogFileOffset;
  }

  public CompletableFuture<?> getFuture() {
    return future;
  }

  public BinlogFileOffset getBinlogFileOffset() {
    return binlogFileOffset;
  }
}
