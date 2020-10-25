package io.eventuate.local.unified.cdc.pipeline.common.factory;

import io.eventuate.common.eventuate.local.BinLogEvent;
import io.eventuate.local.common.BinlogEntryToEventConverter;

import java.util.function.Function;

public interface BinlogEntryToEventConverterFactory<EVENT extends BinLogEvent> extends Function<Long, BinlogEntryToEventConverter<EVENT>> {
}
