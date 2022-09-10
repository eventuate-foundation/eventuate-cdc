package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateJdbcOperationsUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

public class BinlogEntryToPublishedEventConverter implements BinlogEntryToEventConverter<PublishedEvent> {

  public IdGenerator idGenerator;

  public BinlogEntryToPublishedEventConverter(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public Optional<PublishedEvent> convert(BinlogEntry binlogEntry, Integer partitionOffset) {

    if (binlogEntry.getBooleanColumn("published")) {
      return Optional.empty();
    }

    String eventId = binlogEntry.getStringColumn("event_id");

    if (StringUtils.isEmpty(eventId)) {
      Long dbId = binlogEntry.getLongColumn(EventuateJdbcOperationsUtils.EVENT_AUTO_GENERATED_ID_COLUMN);
      eventId = idGenerator.genId(dbId, partitionOffset).asString();
    }

    PublishedEvent publishedEvent = new PublishedEvent(
            eventId,
            binlogEntry.getStringColumn("entity_id"),
            binlogEntry.getStringColumn("entity_type"),
            binlogEntry.getStringColumn("event_data"),
            binlogEntry.getStringColumn("event_type"),
            binlogEntry.getBinlogFileOffset(),
            Optional.ofNullable(binlogEntry.getStringColumn("metadata")));

    return Optional.of(publishedEvent);
  }

  @Override
  public String getDestinationColumn() {
    return "entity_type";
  }
}