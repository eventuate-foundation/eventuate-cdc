package io.eventuate.local.common;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.LeadershipController;

import java.util.UUID;

public class BinlogEntryReaderLeadership {
  private String leaderLockId;
  private LeaderSelectorFactory leaderSelectorFactory;
  private BinlogEntryReader binlogEntryReader;

  private EventuateLeaderSelector eventuateLeaderSelector;
  private volatile boolean leader;
  private LeadershipController leadershipController;

  public BinlogEntryReaderLeadership(String leaderLockId,
                                     LeaderSelectorFactory leaderSelectorFactory,
                                     BinlogEntryReader binlogEntryReader) {

    this.leaderLockId = leaderLockId;
    this.leaderSelectorFactory = leaderSelectorFactory;
    this.binlogEntryReader = binlogEntryReader;

    binlogEntryReader.setRestartCallback(this::restart);
  }

  public void start() {
    eventuateLeaderSelector = leaderSelectorFactory.create(leaderLockId,
            UUID.randomUUID().toString(),
            this::leaderSelectedCallback,
            this::leaderRemovedCallback
  );

    eventuateLeaderSelector.start();
  }

  private void leaderSelectedCallback(LeadershipController leadershipController) {
    this.leadershipController = leadershipController;
    leader = true;
    new Thread(binlogEntryReader::start).start();
  }

  private void leaderRemovedCallback() {
    leader = false;
    binlogEntryReader.stop(false);
  }

  public void stop() {
    binlogEntryReader.stop();
    eventuateLeaderSelector.stop();
  }

  public BinlogEntryReader getBinlogEntryReader() {
    return binlogEntryReader;
  }

  public boolean isLeader() {
    return leader;
  }

  private void restart() {
    leadershipController.stop();
  }
}
