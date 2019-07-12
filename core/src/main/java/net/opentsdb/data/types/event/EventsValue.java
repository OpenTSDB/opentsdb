package net.opentsdb.data.types.event;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

public class EventsValue implements EventType, TimeSeriesValue<EventType> {

  private final String eventId;
  private final TimeStamp timestamp;
  private final TimeStamp endTimestamp;
  private final String source;
  private final String title;
  private final String message;
  private final String priority;
  private final String status;
  private final String userId;
  private final boolean ongoing;
  private final String parentId;
  private final String childId;
  private final Map<String, Object> additionalProps;
  private final String namespace;

  public EventsValue(String eventId, TimeStamp timestamp, TimeStamp endTimestamp, String source,
      String title, String message, String priority, String status, String userId, boolean ongoing,
      String parentId, String childId, Map<String, Object> additionalProps, String namespace) {

    this.eventId = eventId;
    this.timestamp = timestamp;
    this.endTimestamp = endTimestamp;
    this.source = source;
    this.title = title;
    this.message = message;
    this.priority = priority;
    this.status = status;
    this.userId = userId;
    this.ongoing = ongoing;
    this.parentId = parentId;
    this.childId = childId;
    this.additionalProps = additionalProps;
    this.namespace = namespace;
  }

  @Override
  public TimeStamp timestamp() {
    return this.timestamp;
  }

  @Override
  public EventType value() {
    return this;
  }

  @Override
  public String message() {
    return this.message;
  }

  @Override
  public TypeToken<EventType> type() {
    return EventType.TYPE;
  }

  @Override
  public String eventId() {
    return eventId;
  }

  @Override
  public String title() {
    return title;
  }

  @Override
  public String priority() {
    return priority;
  }

  @Override
  public String namespace() {
    return namespace;
  }

  @Override
  public String source() {
    return source;
  }

  @Override
  public String status() {
    return status;
  }

  @Override
  public TimeStamp endTimestamp() {
    return endTimestamp;
  }

  @Override
  public String userId() {
    return userId;
  }

  @Override
  public boolean ongoing() {
    return ongoing;
  }

  @Override
  public String parentId() {
    return parentId;
  }

  @Override
  public String childId() {
    return childId;
  }

  @Override
  public Map<String, Object> additionalProps() {
    return additionalProps;
  }

}
