package io.eventuate.local.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentCountedLinkedQueue<OFFSET> {

  protected ConcurrentLinkedQueue<CompletableFuture<OFFSET>> offsets = new ConcurrentLinkedQueue<>();
  public final AtomicInteger size = new AtomicInteger();

  public boolean add(CompletableFuture<OFFSET> offsetCompletableFuture) {
    size.incrementAndGet();
    return offsets.add(offsetCompletableFuture);
  }

  public CompletableFuture<OFFSET> poll() {
    CompletableFuture<OFFSET> result = offsets.poll();
    if (result != null)
      size.decrementAndGet();
    return result;
  }

  public CompletableFuture<OFFSET> peek() {
    return offsets.peek();
  }

}
