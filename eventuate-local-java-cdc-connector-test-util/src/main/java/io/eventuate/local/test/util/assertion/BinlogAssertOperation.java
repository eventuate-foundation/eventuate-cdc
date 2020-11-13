package io.eventuate.local.test.util.assertion;

import io.eventuate.common.eventuate.local.BinLogEvent;

public interface BinlogAssertOperation<EVENT extends BinLogEvent> {
  void apply(EVENT event);

  default void applyOnlyOnce(EVENT event) {};
}
