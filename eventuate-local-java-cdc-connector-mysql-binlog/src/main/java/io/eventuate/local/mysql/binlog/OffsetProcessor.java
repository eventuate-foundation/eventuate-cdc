package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.db.log.common.OffsetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OffsetProcessor {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private ConcurrentLinkedQueue<CompletableFuture<BinlogFileOffset>> offsets = new ConcurrentLinkedQueue<>();
  private AtomicBoolean processingOffsets = new AtomicBoolean(false);
  private OffsetStore offsetStore;

  public OffsetProcessor(OffsetStore offsetStore) {
    this.offsetStore = offsetStore;
  }

  public void saveOffset(CompletableFuture<BinlogFileOffset> offset) {
    offsets.add(offset);

    offset.whenComplete((o, throwable) -> processOffsets());
  }

  private void processOffsets() {
    while (true) {
      if (!processingOffsets.compareAndSet(false, true)) {
        return;
      }

      Optional<BinlogFileOffset> offset = Optional.empty();

      while (true) {
        if (isDone(offsets.peek())) {
          offset = Optional.of(getOffset(offsets.poll()));
        }
        else {
          break;
        }
      }

      offset.ifPresent(offsetStore::save);

      processingOffsets.set(false);

      //Double check in case if new future offsets become ready to save,
      // but processing was not started, because there is other one was finishing
      if (!isDone(offsets.peek())) {
        return;
      }
    }
  }

  private BinlogFileOffset getOffset(CompletableFuture<BinlogFileOffset> offset) {
    try {
      return offset.get();
    } catch (Throwable t) {
      logger.error("Event publishing failed", t);
      throw new RuntimeException(t);
    }
  }

  private boolean isDone(CompletableFuture<BinlogFileOffset> offset) {
    return offset != null && offset.isDone();
  }
}
