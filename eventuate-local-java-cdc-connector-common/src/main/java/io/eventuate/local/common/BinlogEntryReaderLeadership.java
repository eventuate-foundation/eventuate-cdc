package io.eventuate.local.common;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.LeadershipController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BinlogEntryReaderLeadership {
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private String leaderLockId;
  private LeaderSelectorFactory leaderSelectorFactory;
  private final BinlogEntryReader binlogEntryReader;

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
    logger.info("Starting BinlogEntryReaderLeadership");

    eventuateLeaderSelector = leaderSelectorFactory.create(leaderLockId,
            UUID.randomUUID().toString(),
            this::leaderSelectedCallback,
            this::leaderRemovedCallback);

    eventuateLeaderSelector.start();
  }

  private void leaderSelectedCallback(LeadershipController leadershipController) {
    logger.info("Assigning leadership");
    this.leadershipController = leadershipController;
    leader = true;
    new Thread(binlogEntryReader::start).start();
    logger.info("Assigned leadership");
  }

  private void leaderRemovedCallback() {
    logger.info("Resigning leadership");
    leader = false;
    binlogEntryReader.stop(false);
    logger.info("Resigned leadership");
  }

  public void stop() {
    logger.info("Stopping BinlogEntryReaderLeadership");

    binlogEntryReader.stop();
    eventuateLeaderSelector.stop();

    logger.info("Stopped BinlogEntryReaderLeadership");
  }

  public BinlogEntryReader getBinlogEntryReader() {
    return binlogEntryReader;
  }

  public boolean isLeader() {
    return leader;
  }

  private void restart() {
    logger.info("Restarting BinlogEntryReaderLeadership");

    leadershipController.stop();

    logger.info("Restarted BinlogEntryReaderLeadership");
  }
}
