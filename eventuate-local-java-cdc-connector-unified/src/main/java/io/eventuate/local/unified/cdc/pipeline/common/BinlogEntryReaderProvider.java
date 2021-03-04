package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BinlogEntryReaderProvider {

  /*
    Filled and all readers started from single thread (CdcPipelineConfigurator.start).
    But can be accessed from many threads because of reader's status service.
    So usage of ConcurrentHashMap instead of HashMap should be enough for proper work.
  */
  private ConcurrentMap<String, BinlogEntryReaderLeadership> clients = new ConcurrentHashMap<>();

  public void add(String name, BinlogEntryReaderLeadership binlogEntryReaderLeadership, CdcPipelineReaderProperties properties) {
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
