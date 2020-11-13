package io.eventuate.local.test.util.assertion;

import io.eventuate.common.eventuate.local.BinLogEvent;

public interface BinlogAssertOperationBuilder<EVENT extends BinLogEvent> {
  BinlogAssertOperation<EVENT> build();
}
