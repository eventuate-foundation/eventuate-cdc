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

  private ConcurrentLinkedQueue<CompletableFuture<BinlogFileOffset>> futuresWithOffset = new ConcurrentLinkedQueue<>();
  private AtomicBoolean processingOffsets = new AtomicBoolean(false);
  private OffsetStore offsetStore;

  public OffsetProcessor(OffsetStore offsetStore) {
    this.offsetStore = offsetStore;
  }

  public void saveOffset(CompletableFuture<BinlogFileOffset> futureWithOffset) {
    futuresWithOffset.add(futureWithOffset);

    futureWithOffset.whenComplete((o, throwable) -> processOffsets());
  }

  private void processOffsets() {
    while (true) {
      if (!processingOffsets.compareAndSet(false, true)) {
        return;
      }

      Optional<BinlogFileOffset> offset = Optional.empty();

      while (true) {
        if (isFutureWithOffsetReady(futuresWithOffset.peek())) {
          offset = Optional.of(getOffset(futuresWithOffset.poll()));
        }
        else {
          break;
        }
      }

      offset.ifPresent(offsetStore::save);

      processingOffsets.set(false);

      //Double check in case if new future offsets become ready to save,
      // but processing was not started, because there is other one was finishing
      if (!isFutureWithOffsetReady(futuresWithOffset.peek())) {
        return;
      }
    }
  }

  private BinlogFileOffset getOffset(CompletableFuture<BinlogFileOffset> futureWithOffset) {
    try {
      return futureWithOffset.get();
    } catch (Throwable t) {
      logger.error("Event publishing failed", t);
      throw new RuntimeException(t);
    }
  }

  private boolean isFutureWithOffsetReady(CompletableFuture<BinlogFileOffset> future) {
    return future != null && future.isDone();
  }
}
