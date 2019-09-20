package io.eventuate.local.common;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public class BinlogEntryReaderLeadershipTest {

  @Test
  public void testStartAndStop() {
    LeaderSelectorFactory leaderSelectorFactory = (lockId, leaderId, leaderSelected, leaderRemoved) -> new EventuateLeaderSelector() {
      @Override
      public void start() {
        leaderSelected.run(null);
      }

      @Override
      public void stop() {
        leaderRemoved.run();
      }
    };

    BinlogEntryReader binlogEntryReader = Mockito.mock(BinlogEntryReader.class);

    BinlogEntryReaderLeadership binlogEntryReaderLeadership = new BinlogEntryReaderLeadership(null,
            leaderSelectorFactory,
            binlogEntryReader);

    binlogEntryReaderLeadership.start();

    Mockito.verify(binlogEntryReader).start();
    Mockito.verify(binlogEntryReader, Mockito.never()).stop();

    binlogEntryReaderLeadership.stop();

    Mockito.verify(binlogEntryReader).start();
    Mockito.verify(binlogEntryReader, Mockito.atLeastOnce()).stop();
  }
}
