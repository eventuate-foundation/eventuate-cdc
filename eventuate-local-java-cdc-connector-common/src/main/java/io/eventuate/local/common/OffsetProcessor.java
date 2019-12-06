package io.eventuate.local.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OffsetProcessor<OFFSET> {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private AtomicBoolean processingOffsets = new AtomicBoolean(false);

  protected ConcurrentLinkedQueue<CompletableFuture<OFFSET>> offsets = new ConcurrentLinkedQueue<>();
  protected GenericOffsetStore<OFFSET> offsetStore;

  public OffsetProcessor(GenericOffsetStore<OFFSET> offsetStore) {
    this.offsetStore = offsetStore;
  }

  public void saveOffset(CompletableFuture<OFFSET> offset) {
    offsets.add(offset);

    offset.whenComplete((o, throwable) -> processOffsets());
  }

  private void processOffsets() {
    while (true) {
      if (!processingOffsets.compareAndSet(false, true)) {
        return;
      }

      collectAndSaveOffsets();

      processingOffsets.set(false);

      //Double check in case if new future offsets become ready to save,
      // but processing was not started, because there is other one was finishing
      if (!isDone(offsets.peek())) {
        return;
      }
    }
  }

  protected void collectAndSaveOffsets() {
    Optional<OFFSET> offset = Optional.empty();

    while (true) {
      if (isDone(offsets.peek())) {
        offset = Optional.of(getOffset(offsets.poll()));
      }
      else {
        break;
      }
    }

    offset.ifPresent(offsetStore::save);
  }

  protected OFFSET getOffset(CompletableFuture<OFFSET> offset) {
    try {
      return offset.get();
    } catch (Throwable t) {
      logger.error("Event publishing failed", t);
      throw new RuntimeException(t);
    }
  }

  protected boolean isDone(CompletableFuture<OFFSET> offset) {
    return offset != null && offset.isDone();
  }
}
