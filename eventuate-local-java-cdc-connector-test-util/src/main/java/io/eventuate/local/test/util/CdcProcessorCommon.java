package io.eventuate.local.test.util;

import io.eventuate.common.PublishedEvent;

public interface CdcProcessorCommon {
  default void onEventSent(PublishedEvent publishedEvent) {}
}
