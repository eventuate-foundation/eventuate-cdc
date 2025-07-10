package io.eventuate.local.common;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static io.eventuate.util.test.async.Eventually.eventually;
import static org.mockito.Mockito.*;

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

    BinlogEntryReader binlogEntryReader = mock(BinlogEntryReader.class);

    BinlogEntryReaderLeadership binlogEntryReaderLeadership = new BinlogEntryReaderLeadership(null,
            leaderSelectorFactory,
            binlogEntryReader);

    binlogEntryReaderLeadership.start();

    eventually(() -> {
      verify(binlogEntryReader).start();
      verify(binlogEntryReader, never()).stop();
    });

    binlogEntryReaderLeadership.stop();

    verify(binlogEntryReader).start();
    verify(binlogEntryReader, atLeastOnce()).stop();
  }
}
