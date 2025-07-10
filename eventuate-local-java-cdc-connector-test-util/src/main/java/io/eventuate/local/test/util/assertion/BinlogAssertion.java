package io.eventuate.local.test.util.assertion;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class BinlogAssertion<EVENT extends BinLogEvent> {

  private int waitIterations;
  private int iterationTimeoutMilliseconds;

  private ConcurrentLinkedQueue<EVENT> events = new ConcurrentLinkedQueue<>();

  public BinlogAssertion(int waitIterations, int iterationTimeoutMilliseconds) {
    this.waitIterations = waitIterations;
    this.iterationTimeoutMilliseconds = iterationTimeoutMilliseconds;
  }

  public void assertEventReceived(BinlogAssertOperation<EVENT> assertOperation) {
    AtomicReference<Throwable> throwable = new AtomicReference<>();

    Eventually.eventually(waitIterations, iterationTimeoutMilliseconds, TimeUnit.MILLISECONDS,() -> {
      EVENT event = events.poll();

      Assertions.assertNotNull(event);

      try {
        assertOperation.applyOnlyOnce(event);
      } catch (Throwable t) {
        throwable.set(t);
      }

      if (throwable.get() == null) {
        assertOperation.apply(event);
      }
    });

    if (throwable.get() != null) {
      throw new RuntimeException(throwable.get());
    }
  }

  public void addEvent(EVENT event) {
    events.add(event);
  }
}
