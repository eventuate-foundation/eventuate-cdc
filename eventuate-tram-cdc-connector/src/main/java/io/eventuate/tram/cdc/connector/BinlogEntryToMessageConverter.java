package io.eventuate.tram.cdc.connector;

import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.BinlogEntry;
import io.eventuate.local.common.BinlogEntryToEventConverter;

import java.util.Map;

public class BinlogEntryToMessageConverter implements BinlogEntryToEventConverter<MessageWithDestination> {
  @Override
  public MessageWithDestination convert(BinlogEntry binlogEntry) {
    return new MessageWithDestination(binlogEntry.convertColumnToString("destination"),
            binlogEntry.convertColumnToString("payload"),
            JSonMapper.fromJson(binlogEntry.convertColumnToString("headers"), Map.class),
            binlogEntry.getBinlogFileOffset());
  }
}
