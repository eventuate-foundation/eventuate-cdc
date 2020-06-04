package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.PublishedEvent;

import java.util.Optional;

public class BinlogEntryToPublishedEventConverter implements BinlogEntryToEventConverter<PublishedEvent> {
  @Override
  public PublishedEvent convert(BinlogEntry binlogEntry) {
    return new PublishedEvent(
            binlogEntry.getStringColumn("event_id"),
            binlogEntry.getStringColumn("entity_id"),
            binlogEntry.getStringColumn("entity_type"),
            binlogEntry.getStringColumn("event_data"),
            binlogEntry.getStringColumn("event_type"),
            binlogEntry.getBinlogFileOffset(),
            Optional.ofNullable(binlogEntry.getStringColumn("metadata")));
  }
}