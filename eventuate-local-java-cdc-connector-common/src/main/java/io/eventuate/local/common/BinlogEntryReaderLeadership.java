package io.eventuate.local.common;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;

import java.util.Optional;
import java.util.UUID;

public class BinlogEntryReaderLeadership {
  private String leaderLockId;
  private LeaderSelectorFactory leaderSelectorFactory;
  private BinlogEntryReader binlogEntryReader;

  private EventuateLeaderSelector eventuateLeaderSelector;
  private Optional<Runnable> actionOnStop = Optional.empty();
  private volatile boolean leader;

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
            () -> {
              leader = false;
              binlogEntryReader.start();
            },
            () -> {
              leader = true;
              binlogEntryReader.stop();
              actionOnStop.ifPresent(Runnable::run);
            });

    eventuateLeaderSelector.start();
  }

  public void stop() {
    eventuateLeaderSelector.stop();
    binlogEntryReader.stop();
    binlogEntryReader.clearBinlogEntryHandlers();
  }

  public BinlogEntryReader getBinlogEntryReader() {
    return binlogEntryReader;
  }

  public boolean isLeader() {
    return leader;
  }

  private void restart() {
    eventuateLeaderSelector.stop();
    actionOnStop = Optional.of(() -> {
      actionOnStop = Optional.empty();
      start();
    });
  }
}
