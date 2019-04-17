package io.eventuate.local.common;

import io.eventuate.common.BinlogFileOffset;

public interface PublishingFilter {
  boolean shouldBePublished(BinlogFileOffset sourceBinlogFileOffset, String destinationTopic);
}
