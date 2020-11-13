package io.eventuate.local.test.util;

public class EventInfo {
  private String eventData;
  private String eventId;
  private String entityId;

  public EventInfo(String eventData, String eventId, String entityId) {
    this.eventData = eventData;
    this.eventId = eventId;
    this.entityId = entityId;
  }

  public String getEventData() {
    return eventData;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public String getEntityId() {
    return entityId;
  }

  public void setEntityId(String entityId) {
    this.entityId = entityId;
  }
}
