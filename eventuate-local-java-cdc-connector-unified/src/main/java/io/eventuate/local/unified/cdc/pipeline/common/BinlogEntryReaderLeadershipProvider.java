package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.common.BinlogEntryReaderLeadership;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BinlogEntryReaderLeadershipProvider {

  private Map<String, BinlogEntryReaderLeadership> clients = new HashMap<>();

  public void addBinlogEntryReaderLeadership(String name, BinlogEntryReaderLeadership binlogEntryReaderLeadership) {
    clients.put(name.toLowerCase(), binlogEntryReaderLeadership);
  }

  public BinlogEntryReaderLeadership getBinlogEntryReaderLeadership(String name) {
    return clients.get(name.toLowerCase());
  }

  public void start() {
    clients.values().forEach(BinlogEntryReaderLeadership::start);
  }

  public Collection<BinlogEntryReaderLeadership> getAllBinlogEntryReaderLeadershipInstances() {
    return clients.values();
  }

  public void stop() {
    clients.values().forEach(BinlogEntryReaderLeadership::stop);
  }
}
