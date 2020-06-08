package io.eventuate.tram.cdc.connector;

import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.BinlogEntry;
import io.eventuate.local.common.BinlogEntryToEventConverter;

import java.util.Map;

public class BinlogEntryToMessageConverter implements BinlogEntryToEventConverter<MessageWithDestination> {
  @Override
  public MessageWithDestination convert(BinlogEntry binlogEntry) {
    return new MessageWithDestination(binlogEntry.getStringColumn("destination"),
            binlogEntry.getJsonColumn("payload"),
            JSonMapper.fromJson(binlogEntry.getJsonColumn("headers"), Map.class),
            binlogEntry.getBinlogFileOffset());
  }
}
