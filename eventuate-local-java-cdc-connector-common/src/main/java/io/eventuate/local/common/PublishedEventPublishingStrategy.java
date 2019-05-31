package io.eventuate.local.common;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.messaging.kafka.common.AggregateTopicMapping;

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
