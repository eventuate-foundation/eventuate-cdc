package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.common.OffsetProcessor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MySqlBinlogOffsetProcessor {
  private OffsetProcessor<BinlogFileOffset> offsetProcessor;

  public MySqlBinlogOffsetProcessor(OffsetProcessor<BinlogFileOffset> offsetProcessor) {
    this.offsetProcessor = offsetProcessor;
  }

  public void saveTableMapOffset(BinlogFileOffset binlogFileOffset) {
    saveOffset(binlogFileOffset);
  }

  public void saveWriteRowsOffset(CompletableFuture<BinlogFileOffset> futureWithOffset) {
    CompletableFuture<Optional<BinlogFileOffset>> futureWithEmptyOffset = new CompletableFuture<>();

    futureWithOffset.whenComplete((binlogFileOffset, throwable) -> {
      if (throwable == null) {
        futureWithEmptyOffset.complete(Optional.empty());
      } else {
        futureWithEmptyOffset.completeExceptionally(throwable);
      }
    });

    offsetProcessor.saveOffset(futureWithEmptyOffset);
  }

  public void saveXidOffset(BinlogFileOffset binlogFileOffset) {
    saveOffset(binlogFileOffset);
  }

  private void saveOffset(BinlogFileOffset binlogFileOffset) {
    offsetProcessor.saveOffset(CompletableFuture.completedFuture(Optional.of(binlogFileOffset)));
  }
}
