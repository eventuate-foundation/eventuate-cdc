package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.db.log.common.OffsetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OffsetProcessor {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private ConcurrentLinkedQueue<FutureOffset> futureOffsets = new ConcurrentLinkedQueue<>();
  private AtomicBoolean processingOffsets = new AtomicBoolean(false);
  private OffsetStore offsetStore;

  public OffsetProcessor(OffsetStore offsetStore) {
    this.offsetStore = offsetStore;
  }

  public void saveOffset(CompletableFuture<?> future, BinlogFileOffset binlogFileOffset) {
    futureOffsets.add(new FutureOffset(future, binlogFileOffset));

    future.whenComplete((o, throwable) -> processOffsets());
  }

  private void processOffsets() {
    if (!processingOffsets.compareAndSet(false, true)) {
      return;
    }

    FutureOffset previousFutureOffset = null;

    while (true) {
      FutureOffset futureOffset = futureOffsets.peek();

      if (previousFutureOffset == null) {
        if (!isFutureOffsetReadyToSave(futureOffset)) {
          break;
        }

        previousFutureOffset = checkFutureForException(futureOffsets.poll());

        continue;
      }

      if (!isFutureOffsetReadyToSave(futureOffset)) {
        offsetStore.save(previousFutureOffset.getBinlogFileOffset());
        break;
      }

      previousFutureOffset = checkFutureForException(futureOffsets.poll());
    }

    processingOffsets.set(false);

    restartProcessingIfNeccary();
  }


  private void restartProcessingIfNeccary() {
    //Double check in case if new future offsets become ready to save,
    // but procissing was not started, because there is other one was finishing
    if (isFutureOffsetReadyToSave(futureOffsets.peek())) {
      if (processingOffsets.compareAndSet(false, true)) {
        //To prevent stack overflow
        CompletableFuture.runAsync(this::processOffsets);
      }
    }
  }

  private FutureOffset checkFutureForException(FutureOffset futureOffset) {
    try {
      futureOffset.getFuture().get();
    } catch (Throwable t) {
      logger.error("Event publishing failed", t);
      throw new RuntimeException(t);
    }

    return futureOffset;
  }

  private boolean isFutureOffsetReadyToSave(FutureOffset futureOffset) {
    return futureOffset != null && futureOffset.getFuture().isDone();
  }
}
