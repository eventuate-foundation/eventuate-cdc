package io.eventuate.local.common;

import io.eventuate.Int128;
import io.eventuate.common.AggregateTopicMapping;
import io.eventuate.common.PublishedEvent;
import io.eventuate.javaclient.commonimpl.JSonMapper;

import java.util.Optional;

public class PublishedEventPublishingStrategy implements PublishingStrategy<PublishedEvent> {

  @Override
  public String partitionKeyFor(PublishedEvent publishedEvent) {
    return publishedEvent.getEntityId();
  }

  @Override
  public String topicFor(PublishedEvent publishedEvent) {
    return AggregateTopicMapping.aggregateTypeToTopic(publishedEvent.getEntityType());
  }

  @Override
  public String toJson(PublishedEvent eventInfo) {
    return JSonMapper.toJson(eventInfo);
  }

  @Override
  public Optional<Long> getCreateTime(PublishedEvent publishedEvent) {
// TODO: Original implementation is based in Int128 id, which should not be required.
//    return Optional.of(Int128.fromString(publishedEvent.getId()).getHi());
    return Optional.empty();
  }

}
