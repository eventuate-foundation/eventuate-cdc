package io.eventuate.local.mysql.binlog;

import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class OffsetProcessorTest {

  private Optional<Long> lastSavedEventOffset;
  private OffsetProcessor offsetProcessor;

  private int nEvents = 1000;
  private long timeoutDenominator = 100;

  @Test
  public void testFixedTimeoutOfFutureWithOffset() {
    runTest(createFixedTimeoutGenerator());
  }

  @Test
  public void testRandomTimeoutOfFutureWithOffset() {
    runTest(createRandomTimeoutGenerator());
  }

  @Test
  public void testAscendingTimeoutOfFutureWithOffset() {
    runTest(createAscendingTimeoutGenerator());
  }

  @Test
  public void testDecendingTimeoutOfFutureWithOffset() {
    runTest(createDecendingTimeoutGenerator());
  }

  private void runTest(Function<Integer, Long> timeoutGenerator) {
    createOffsetProcessor();
    generateEvents(nEvents, timeoutGenerator);
    assertEventOffsetsProcessedProperly();
  }

  private void assertEventOffsetsProcessedProperly() {
    Eventually.eventually(() -> Assert.assertEquals(nEvents, (long) lastSavedEventOffset.orElse(-1L)));
  }

  private void createOffsetProcessor() {
    offsetProcessor = new OffsetProcessor(createOffsetStore());
  }

  private OffsetStore createOffsetStore() {
    lastSavedEventOffset = Optional.empty();

    return new OffsetStore() {

      private boolean orderIsCorrect = true;

      @Override
      public Optional<BinlogFileOffset> getLastBinlogFileOffset() {
        return Optional.empty();
      }

      @Override
      public void save(BinlogFileOffset binlogFileOffset) {
        if (!orderIsCorrect) {
          return;
        }

        long eventOffset = binlogFileOffset.getOffset();

        orderIsCorrect = lastSavedEventOffset.map(last -> last < eventOffset).orElse(true);

        lastSavedEventOffset = Optional.of(eventOffset);
      }
    };
  }

  private void generateEvents(int nEvents, Function<Integer, Long> timeoutGenerator) {
    for (int i = 1; i <= nEvents; i++) {
      int eventOffset = i;

      CompletableFuture<BinlogFileOffset> future = CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(timeoutGenerator.apply(eventOffset));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return new BinlogFileOffset("", eventOffset);
      });

      offsetProcessor.saveOffset(future);
    }
  }

  private Function<Integer, Long> createFixedTimeoutGenerator() {
    return eventOffset -> 1L;
  }

  private Function<Integer, Long> createRandomTimeoutGenerator() {
    return eventOffset -> eventOffset % timeoutDenominator;
  }

  private Function<Integer, Long> createAscendingTimeoutGenerator() {
    return eventOffset -> eventOffset / timeoutDenominator;
  }

  private Function<Integer, Long> createDecendingTimeoutGenerator() {
    return eventOffset -> nEvents / timeoutDenominator - eventOffset / timeoutDenominator;
  }
}
