package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.BinlogFileOffset;

public interface PublishingFilter {
  boolean shouldBePublished(BinlogFileOffset sourceBinlogFileOffset, String destinationTopic);
}
