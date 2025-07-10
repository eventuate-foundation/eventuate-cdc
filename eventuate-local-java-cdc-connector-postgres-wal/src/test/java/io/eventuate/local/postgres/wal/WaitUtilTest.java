package io.eventuate.local.postgres.wal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WaitUtilTest {

  @Test
  public void testSuccessfulWaiting() throws InterruptedException {
    WaitUtil waitUtil = new WaitUtil(10000);

    Assertions.assertTrue(waitUtil.start());
    Assertions.assertFalse(waitUtil.start());

    Thread.sleep(1);
    Assertions.assertTrue(waitUtil.tick());
    Assertions.assertTrue(waitUtil.isWaiting());

    Thread.sleep(1);
    Assertions.assertTrue(waitUtil.tick());
    Assertions.assertTrue(waitUtil.isWaiting());
  }

  @Test
  public void testFailedWaiting() throws InterruptedException {
    WaitUtil waitUtil = new WaitUtil(1);
    waitUtil.start();
    Thread.sleep(10);
    Assertions.assertFalse(waitUtil.tick());
    Assertions.assertFalse(waitUtil.isWaiting());
  }

}