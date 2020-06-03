package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.PublishedEvent;

import java.util.Optional;

public class BinlogEntryToPublishedEventConverter implements BinlogEntryToEventConverter<PublishedEvent> {
  @Override
  public PublishedEvent convert(BinlogEntry binlogEntry) {
    return new PublishedEvent(
            binlogEntry.convertColumnToString("event_id"),
            binlogEntry.convertColumnToString("entity_id"),
            binlogEntry.convertColumnToString("entity_type"),
            binlogEntry.convertColumnToString("event_data"),
            binlogEntry.convertColumnToString("event_type"),
            binlogEntry.getBinlogFileOffset(),
            Optional.ofNullable(binlogEntry.convertColumnToString("metadata")));
  }
}