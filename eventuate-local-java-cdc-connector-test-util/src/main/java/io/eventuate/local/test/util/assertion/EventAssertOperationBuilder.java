package io.eventuate.local.test.util.assertion;

import io.eventuate.common.eventuate.local.PublishedEvent;
import io.eventuate.local.test.util.EventInfo;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

public class EventAssertOperationBuilder implements BinlogAssertOperationBuilder<PublishedEvent> {
  private String id;
  private String entityId;
  private String data;

  private List<String> excludedIds = new ArrayList<>();

  public static EventAssertOperationBuilder assertion() {
    return new EventAssertOperationBuilder();
  }

  public static EventAssertOperationBuilder fromEventInfo(EventInfo eventInfo) {
    return assertion().withId(eventInfo.getEventId()).withEntityId(eventInfo.getEntityId()).withData(eventInfo.getEventData());
  }

  @Override
  public BinlogAssertOperation<PublishedEvent> build() {

    return new BinlogAssertOperation<PublishedEvent>() {
      @Override
      public void apply(PublishedEvent event) {
        if (id != null) {
          Assertions.assertEquals(id, event.getId());
        }

        if (entityId != null) {
          Assertions.assertEquals(entityId, event.getEntityId());
        }

        if (data != null) {
          Assertions.assertEquals(data, event.getEventData());
        }
      }

      @Override
      public void applyOnlyOnce(PublishedEvent event) {
        for (String id : excludedIds) {
          Assertions.assertNotEquals(id, event.getId());
        }
      }
    };
  }

  public EventAssertOperationBuilder withId(String id) {
    this.id = id;

    return this;
  }

  public EventAssertOperationBuilder excludeId(String id) {
    excludedIds.add(id);

    return this;
  }

  public EventAssertOperationBuilder excludeIds(List<String> ids) {
    excludedIds.addAll(ids);

    return this;
  }

  public EventAssertOperationBuilder withEntityId(String entityId) {
    this.entityId = entityId;

    return this;
  }

  public EventAssertOperationBuilder withData(String data) {
    this.data = data;

    return this;
  }
}
