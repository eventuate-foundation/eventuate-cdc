package io.eventuate.local.unified.cdc.pipeline.common;

import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BinlogEntryReaderProvider {

  private Map<String, ReaderWithProperties> clients = new HashMap<>();

  public void add(String name, BinlogEntryReaderLeadership binlogEntryReaderLeadership, CdcPipelineReaderProperties properties) {
    clients.put(name.toLowerCase(), new ReaderWithProperties(binlogEntryReaderLeadership, properties));
  }

  public BinlogEntryReaderLeadership get(String name) {
    return clients.get(name.toLowerCase()).getBinlogEntryReaderLeadership();
  }

  public CdcPipelineReaderProperties getReaderProperties(String name) {
    return clients.get(name.toLowerCase()).getProperties();
  }

  public void start() {
    clients.values().forEach(ReaderWithProperties::start);
  }

  public Collection<BinlogEntryReaderLeadership> getAll() {
    return clients.values().stream().map(ReaderWithProperties::getBinlogEntryReaderLeadership).collect(Collectors.toList());
  }

  public void stop() {
    clients.values().forEach(ReaderWithProperties::stop);
  }
}

class ReaderWithProperties {
  private BinlogEntryReaderLeadership binlogEntryReaderLeadership;
  private CdcPipelineReaderProperties properties;

  public ReaderWithProperties(BinlogEntryReaderLeadership binlogEntryReaderLeadership, CdcPipelineReaderProperties properties) {
    this.binlogEntryReaderLeadership = binlogEntryReaderLeadership;
    this.properties = properties;
  }

  public BinlogEntryReaderLeadership getBinlogEntryReaderLeadership() {
    return binlogEntryReaderLeadership;
  }

  public CdcPipelineReaderProperties getProperties() {
    return properties;
  }

  public void start() {
    binlogEntryReaderLeadership.start();
  }

  public void stop() {
    binlogEntryReaderLeadership.stop();
  }
}