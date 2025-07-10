package io.eventuate.local.mysql.binlog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.common.OffsetProcessor;
import io.eventuate.local.db.log.common.OffsetStore;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetProcessorTest {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private OffsetStore offsetStore;
  private OffsetProcessor<BinlogFileOffset> offsetProcessor;

  private CompletableFuture<Optional<BinlogFileOffset>> futureOffset1;
  private CompletableFuture<Optional<BinlogFileOffset>> futureOffset2;
  private CompletableFuture<Optional<BinlogFileOffset>> futureOffset3;

  private BinlogFileOffset offset1;
  private BinlogFileOffset offset2;
  private BinlogFileOffset offset3;

  @Test
  public void testRegularOrder() {
    logger.info("Testing regular order");
    createAllCombinationsOfOffsetSavingMap().forEach(this::testRegularOrder);
  }

  private void testRegularOrder(List<Boolean> offsetSavingMap) {
    init();

    int saveInvocationCountWithOffsetA = offsetSavingMap.get(0) ? 1 : 0;
    int saveInvocationCountWithOffsetB = offsetSavingMap.get(1) ? 1 : 0;
    int saveInvocationCountWithOffsetC = offsetSavingMap.get(2) ? 1 : 0;

    logger.info("Saving map: {}, invocation map: {}",
            offsetSavingMap, ImmutableList.of(saveInvocationCountWithOffsetA, saveInvocationCountWithOffsetB, saveInvocationCountWithOffsetC));

    assertUnprocessedCountEquals(3);
    futureOffset1.complete(prepareOffset(offset1, offsetSavingMap.get(0)));
    verify(ImmutableMap.of(offset1, saveInvocationCountWithOffsetA, offset2, 0, offset3, 0));

    assertUnprocessedCountEquals(2);

    futureOffset2.complete(prepareOffset(offset2, offsetSavingMap.get(1)));
    verify(ImmutableMap.of(offset1, saveInvocationCountWithOffsetA, offset2, saveInvocationCountWithOffsetB, offset3, 0));

    assertUnprocessedCountEquals(1);

    futureOffset3.complete(prepareOffset(offset3, offsetSavingMap.get(2)));
    verify(ImmutableMap.of(offset1, saveInvocationCountWithOffsetA, offset2, saveInvocationCountWithOffsetB, offset3, saveInvocationCountWithOffsetC));

    assertUnprocessedCountEquals(0);
  }

  private void assertUnprocessedCountEquals(int i) {
    Eventually.eventually(300, 10, TimeUnit.MILLISECONDS,
            () -> assertEquals(i, offsetProcessor.getUnprocessedOffsetCount().get()));
  }

  @Test
  public void testReversedOrder() {
    logger.info("Testing reversed order");
    createAllCombinationsOfOffsetSavingMap().forEach(this::testReversedOrder);
  }

  private void testReversedOrder(List<Boolean> offsetSavingMap) {
    init();

    futureOffset3.complete(prepareOffset(offset3, offsetSavingMap.get(2)));
    assertUnprocessedCountEquals(3);
    futureOffset2.complete(prepareOffset(offset2, offsetSavingMap.get(1)));
    assertUnprocessedCountEquals(3);
    futureOffset1.complete(prepareOffset(offset1, offsetSavingMap.get(0)));
    assertUnprocessedCountEquals(0);

    int saveInvocationCountWithOffsetA = offsetSavingMap.get(0) && !offsetSavingMap.get(1) && !offsetSavingMap.get(2) ? 1 : 0;
    int saveInvocationCountWithOffsetB = offsetSavingMap.get(1) && !offsetSavingMap.get(2) ? 1 : 0;
    int saveInvocationCountWithOffsetC = offsetSavingMap.get(2) ? 1 : 0;

    logger.info("Saving map: {}, invocation map: {}",
            offsetSavingMap, ImmutableList.of(saveInvocationCountWithOffsetA, saveInvocationCountWithOffsetB, saveInvocationCountWithOffsetC));

    verify(ImmutableMap.of(offset1, saveInvocationCountWithOffsetA, offset2, saveInvocationCountWithOffsetB, offset3, saveInvocationCountWithOffsetC));
  }

  private void init() {
    offsetStore = Mockito.mock(OffsetStore.class);
    offsetProcessor = new OffsetProcessor<>(offsetStore, null);

    futureOffset1 = new CompletableFuture<>();
    futureOffset2 = new CompletableFuture<>();
    futureOffset3 = new CompletableFuture<>();

    offset1 = new BinlogFileOffset("", 1);
    offset2 = new BinlogFileOffset("", 2);
    offset3 = new BinlogFileOffset("", 3);

    offsetProcessor.saveOffset(futureOffset1);
    offsetProcessor.saveOffset(futureOffset2);
    offsetProcessor.saveOffset(futureOffset3);
  }

  private List<List<Boolean>> createAllCombinationsOfOffsetSavingMap() {
    List<List<Boolean>> maps = new ArrayList<>();

    for (int i = 0; i < 8; i++) {
      LinkedList<Boolean> savingMap =
              Integer.toBinaryString(i).chars().boxed().map(c -> c == '1').collect(Collectors.toCollection(LinkedList::new));

      while (savingMap.size() < 3) savingMap.addFirst(false);

      maps.add(savingMap);
    }

    return maps;
  }

  private void verify(Map<BinlogFileOffset, Integer> invocations) {
    Eventually.eventually(300, 10, TimeUnit.MILLISECONDS,
            () -> invocations.forEach((offset, invocationCount) -> Mockito.verify(offsetStore, Mockito.times(invocationCount)).save(offset)));
  }

  private Optional<BinlogFileOffset> prepareOffset(BinlogFileOffset binlogFileOffset, boolean save) {
    if (save) {
      return Optional.of(binlogFileOffset);
    } else {
      return Optional.empty();
    }
  }
}
