package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateCommonJdbcOperations;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

public class BinlogEntryToPublishedEventConverter implements BinlogEntryToEventConverter<PublishedEvent> {

  public IdGenerator idGenerator;

  public BinlogEntryToPublishedEventConverter(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public PublishedEvent convert(BinlogEntry binlogEntry) {

    String eventId = binlogEntry.getStringColumn("event_id");

    if (StringUtils.isEmpty(eventId)) {
      Long dbId = binlogEntry.getLongColumn(EventuateCommonJdbcOperations.EVENT_AUTO_GENERATED_ID_COLUMN);
      eventId = idGenerator.genId(dbId).asString();
    }

    return new PublishedEvent(
            eventId,
            binlogEntry.getStringColumn("entity_id"),
            binlogEntry.getStringColumn("entity_type"),
            binlogEntry.getStringColumn("event_data"),
            binlogEntry.getStringColumn("event_type"),
            binlogEntry.getBinlogFileOffset(),
            Optional.ofNullable(binlogEntry.getStringColumn("metadata")));
  }
}