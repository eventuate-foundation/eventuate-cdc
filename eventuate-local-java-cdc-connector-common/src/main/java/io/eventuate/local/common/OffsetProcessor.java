package io.eventuate.local.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class OffsetProcessor<OFFSET> {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private AtomicBoolean processingOffsets = new AtomicBoolean(false);

  protected ConcurrentCountedLinkedQueue<Optional<OFFSET>> offsets = new ConcurrentCountedLinkedQueue<>();
  protected GenericOffsetStore<OFFSET> offsetStore;
  protected Consumer<Exception> offsetSavingExceptionHandler;
  private Executor executor = Executors.newCachedThreadPool();

  public OffsetProcessor(GenericOffsetStore<OFFSET> offsetStore, Consumer<Exception> offsetSavingExceptionHandler) {
    this.offsetStore = offsetStore;
    this.offsetSavingExceptionHandler = offsetSavingExceptionHandler;
  }

  public void saveOffset(CompletableFuture<Optional<OFFSET>> offset) {
    offsets.add(offset);

    offset.whenCompleteAsync(this::processOffsetsWithExceptionHandling, executor);
  }

  private void processOffsetsWithExceptionHandling(Optional<OFFSET> offset, Throwable t) {
    try {
      processOffsets();
    } catch (Exception e) {
      offsetSavingExceptionHandler.accept(e);
    }
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
    Optional<OFFSET> offsetToSave = Optional.empty();

    while (true) {
      if (isDone(offsets.peek())) {
        Optional<OFFSET> offset = getOffset(offsets.poll());

        if (offset.isPresent()) {
          offsetToSave = offset;
        }
      }
      else {
        break;
      }
    }

    offsetToSave.ifPresent(offsetStore::save);
  }

  protected Optional<OFFSET> getOffset(CompletableFuture<Optional<OFFSET>> offset) {
    return CompletableFutureUtil.get(offset);
  }

  protected boolean isDone(CompletableFuture<Optional<OFFSET>> offset) {
    return offset != null && offset.isDone();
  }

  public AtomicInteger getUnprocessedOffsetCount() {
    return offsets.size;
  }
}
