package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.common.BinlogEntryReaderLeadership;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BinlogEntryReaderProvider {

  private Map<String, BinlogEntryReaderLeadership> clients = new HashMap<>();

  public void add(String name, BinlogEntryReaderLeadership binlogEntryReaderLeadership) {
    clients.put(name.toLowerCase(), binlogEntryReaderLeadership);
  }

  public BinlogEntryReaderLeadership get(String name) {
    return clients.get(name.toLowerCase());
  }

  public void start() {
    clients.values().forEach(BinlogEntryReaderLeadership::start);
  }

  public Collection<BinlogEntryReaderLeadership> getAll() {
    return clients.values();
  }

  public void stop() {
    clients.values().forEach(BinlogEntryReaderLeadership::stop);
  }
}
