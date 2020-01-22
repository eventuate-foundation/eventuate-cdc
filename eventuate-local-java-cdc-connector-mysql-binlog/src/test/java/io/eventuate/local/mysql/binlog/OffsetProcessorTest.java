package io.eventuate.local.mysql.binlog;

import com.google.common.collect.ImmutableMap;
import io.eventuate.common.eventuate.local.BinlogFileOffset;
import io.eventuate.local.common.OffsetProcessor;
import io.eventuate.local.db.log.common.OffsetStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public class OffsetProcessorTest {

  private OffsetStore offsetStore;
  private OffsetProcessor<BinlogFileOffset> offsetProcessor;

  private CompletableFuture<BinlogFileOffset> a;
  private CompletableFuture<BinlogFileOffset> b;
  private CompletableFuture<BinlogFileOffset> c;

  private BinlogFileOffset oa;
  private BinlogFileOffset ob;
  private BinlogFileOffset oc;

  @Before
  public void init() {
    offsetStore = Mockito.mock(OffsetStore.class);
    offsetProcessor = new OffsetProcessor<>(offsetStore);

    a = new CompletableFuture<>();
    b = new CompletableFuture<>();
    c = new CompletableFuture<>();

    oa = new BinlogFileOffset("", 1);
    ob = new BinlogFileOffset("", 2);
    oc = new BinlogFileOffset("", 3);

    offsetProcessor.saveOffset(a);
    offsetProcessor.saveOffset(b);
    offsetProcessor.saveOffset(c);
  }

  @Test
  public void testRegularOrder() {
    assertUnprocessedCountEquals(3);
    a.complete(oa);
    verify(ImmutableMap.of(oa, 1, ob, 0, oc, 0));

    assertUnprocessedCountEquals(2);

    b.complete(ob);
    verify(ImmutableMap.of(oa, 1, ob, 1, oc, 0));

    assertUnprocessedCountEquals(1);

    c.complete(oc);
    verify(ImmutableMap.of(oa, 1, ob, 1, oc, 1));

    assertUnprocessedCountEquals(0);
  }

  private void assertUnprocessedCountEquals(int i) {
    assertEquals(i, offsetProcessor.getUnprocessedOffsetCount().get());
  }

  @Test
  public void testReversedOrder() {
    c.complete(oc);
    assertUnprocessedCountEquals(3);
    b.complete(ob);
    assertUnprocessedCountEquals(3);
    a.complete(oa);
    assertUnprocessedCountEquals(0);

    verify(ImmutableMap.of(oa, 0, ob, 0, oc, 1));
  }

  private void verify(Map<BinlogFileOffset, Integer> invocations) {
    invocations.forEach((offset, invocationCount) -> Mockito.verify(offsetStore, Mockito.times(invocationCount)).save(offset));
  }

}
