package io.eventuate.tram.cdc.connector;

import io.eventuate.common.id.IdGenerator;
import io.eventuate.common.jdbc.EventuateJdbcOperationsUtils;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.local.common.BinlogEntry;
import io.eventuate.local.common.BinlogEntryToEventConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BinlogEntryToMessageConverter implements BinlogEntryToEventConverter<MessageWithDestination> {

  public IdGenerator idGenerator;

  public BinlogEntryToMessageConverter(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public Optional<MessageWithDestination> convert(BinlogEntry binlogEntry) {

    if (binlogEntry.getBooleanColumn("published")) {
      return Optional.empty();
    }

    Map<String, String> headers = JSonMapper.fromJson(binlogEntry.getJsonColumn("headers"), Map.class);

    if (!headers.containsKey("ID")) {
      headers = new HashMap<>(headers);

      String generatedId = idGenerator
              .genId(binlogEntry.getLongColumn(EventuateJdbcOperationsUtils.MESSAGE_AUTO_GENERATED_ID_COLUMN))
              .asString();

      headers.put("ID", generatedId);
    }

    MessageWithDestination message = new MessageWithDestination(binlogEntry.getStringColumn("destination"),
            binlogEntry.getJsonColumn("payload"),
            headers,
            binlogEntry.getBinlogFileOffset());

    return Optional.of(message);
  }

  @Override
  public String getDestinationColumn() {
    return "destination";
  }
}
