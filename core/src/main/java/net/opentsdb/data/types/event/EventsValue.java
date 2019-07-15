// This file is part of OpenTSDB.
// Copyright (C) 2019 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net.opentsdb.data.types.event;

import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

public class EventsValue implements EventType, TimeSeriesValue<EventType> {

  /** Unique ID of the event */
  private final String eventId;

  /** Timestamp of the event trigger */
  private final TimeStamp timestamp;

  /** Timestamp of when the event has ended/will end */
  private final TimeStamp endTimestamp;

  /** Source of the event, could be an agent emitting */
  private final String source;

  /** Event title */
  private final String title;

  /** Event content */
  private final String message;

  /** Priority of the event, low/high/medium etc */
  private final String priority;

  /** Status of the event, good/bad etc */
  private final String status;

  /** User of the event */
  private final String userId;

  /** Whether the event is still on going */
  private final boolean ongoing;

  /** List of parent IDs this event can map to */
  private final List<String> parentId;

  /** List of child IDs this event can map to */
  private final List<String> childId;

  /** Additional properties */
  private final Map<String, Object> additionalProps;

  /** Namespace if necessary */
  private final String namespace;

  protected EventsValue(final Builder builder) {
    this.eventId = builder.eventId;
    this.timestamp = builder.timestamp;
    this.endTimestamp = builder.endTimestamp;
    this.source = builder.source;
    this.title = builder.title;
    this.message = builder.message;
    this.priority = builder.priority;
    this.status = builder.status;
    this.userId = builder.userId;
    this.ongoing = builder.ongoing;
    this.parentId = builder.parentId;
    this.childId = builder.childId;
    this.additionalProps = builder.additionalProps;
    this.namespace = builder.namespace;
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
  public List<String> parentId() {
    return parentId;
  }

  @Override
  public List<String> childId() {
    return childId;
  }

  @Override
  public Map<String, Object> additionalProps() {
    return additionalProps;
  }

  public static class Builder {
    /** Unique ID of the event */
    private String eventId;

    /** Timestamp of the event trigger */
    private TimeStamp timestamp;

    /** Timestamp of when the event has ended/will end */
    private TimeStamp endTimestamp;

    /** Source of the event, could be an agent emitting */
    private String source;

    /** Event title */
    private String title;

    /** Event content */
    private String message;

    /** Priority of the event, low/high/medium etc */
    private String priority;

    /** Status of the event, good/bad etc */
    private String status;

    /** User of the event */
    private String userId;

    /** Whether the event is still on going */
    private boolean ongoing;

    /** List of parent IDs this event can map to */
    private List<String> parentId;

    /** List of child IDs this event can map to */
    private List<String> childId;

    /** Additional properties */
    private Map<String, Object> additionalProps;

    /** Namespace if necessary */
    private String namespace;

    Builder() {

    }

    public Builder self() {
      return this;
    }

    /**
     * @param eventId Unique ID of the event
     * @return The builder.
     */
    public Builder setEventId(final String eventId) {
      this.eventId = eventId;
      return this;
    }

    /**
     * @param timestamp Timestamp of the event trigger
     * @return The builder.
     */
    public Builder setTimestamp(final TimeStamp timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    /**
     * @param endTimestamp Timestamp of when the event has ended/will end
     * @return The builder.
     */
    public Builder setEndTimestamp(final TimeStamp endTimestamp) {
      this.endTimestamp = endTimestamp;
      return this;
    }

    /**
     * @param source Source of the event, could be an agent emitting.
     * @return The builder.
     */
    public Builder setSource(final String source) {
      this.source = source;
      return this;
    }

    /**
     * @param title Event title
     * @return The builder.
     */
    public Builder setTitle(final String title) {
      this.title = title;
      return this;
    }

    /**
     * @param message Event message
     * @return The builder.
     */
    public Builder setMessage(final String message) {
      this.message = message;
      return this;
    }

    /**
     * 
     * @param priority Priority of the event, low/high/medium etc
     * @return The builder.
     */
    public Builder setPriority(final String priority) {
      this.priority = priority;
      return this;
    }

    /**
     * 
     * @param status Status of the event, good/bad etc
     * @return The builder.
     */
    public Builder setStatus(final String status) {
      this.status = status;
      return this;
    }

    /**
     * 
     * @param userId User of the event
     * @return The builder.
     */
    public Builder setUserId(final String userId) {
      this.userId = userId;
      return this;
    }

    /**
     * 
     * @param ongoing Whether the event is still on going
     * @return The builder.
     */
    public Builder setOngoing(final boolean ongoing) {
      this.ongoing = ongoing;
      return this;
    }

    /**
     * 
     * @param parentId List of parent IDs this event can map to
     * @return The builder.
     */
    public Builder setParentId(final List<String> parentId) {
      this.parentId = parentId;
      return this;
    }

    /**
     * 
     * @param childId List of child IDs this event can map to
     * @return The builder.
     */
    public Builder setChildId(final List<String> childId) {
      this.childId = childId;
      return this;
    }

    /**
     * 
     * @param additionalProps Additional properties
     * @return The builder.
     */
    public Builder setAdditionalProps(final Map<String, Object> additionalProps) {
      this.additionalProps = additionalProps;
      return this;
    }

    /**
     * 
     * @param namespace Namespace if necessary
     * @return The builder.
     */
    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public EventsValue build() {
      return new EventsValue(this);
    }
  }

}
