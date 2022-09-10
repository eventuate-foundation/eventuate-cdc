package io.eventuate.local.common;

import java.util.Optional;

public interface BinlogEntryToEventConverter<EVENT> {
  Optional<EVENT> convert(BinlogEntry binlogEntry, Integer partitionOffset);

  String getDestinationColumn();
}
