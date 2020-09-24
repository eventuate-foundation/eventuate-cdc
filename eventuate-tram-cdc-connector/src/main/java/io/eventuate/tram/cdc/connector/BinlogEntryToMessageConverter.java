package io.eventuate.tram.cdc.connector;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.BinlogEntry;
import io.eventuate.local.common.BinlogEntryToEventConverter;

import java.util.HashMap;
import java.util.Map;

public class BinlogEntryToMessageConverter implements BinlogEntryToEventConverter<MessageWithDestination> {

  public IdGenerator idGenerator;

  public BinlogEntryToMessageConverter(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public MessageWithDestination convert(BinlogEntry binlogEntry) {

    Map<String, String> headers = JSonMapper.fromJson(binlogEntry.getJsonColumn("headers"), Map.class);

    if (!headers.containsKey("ID")) {
      headers = new HashMap<>(headers);
      headers.put("ID", idGenerator.genId(binlogEntry.getLongColumn("xid")).asString());
    }

    return new MessageWithDestination(binlogEntry.getStringColumn("destination"),
            binlogEntry.getJsonColumn("payload"),
            headers,
            binlogEntry.getBinlogFileOffset());
  }
}
