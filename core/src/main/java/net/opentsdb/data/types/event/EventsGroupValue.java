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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.reflect.TypeToken;
import java.util.Map;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = EventsGroupValue.Builder.class)
public class EventsGroupValue implements EventGroupType, TimeSeriesValue<EventGroupType> {


  /** Key value pairs of a group */
  private final Map<String, String> group;

  /** The latest event */
  private final EventsValue event;

  public EventsGroupValue(final Builder builder) {
    this.group = builder.group;
    this.event = builder.event;
  }

  @Override
  public Map<String, String> group() {
    return group;
  }

  @Override
  public EventsValue event() {
    return event;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public TimeStamp timestamp() {
    return null;
  }

  @Override
  public EventGroupType value() {
    return this;
  }

  @Override
  public TypeToken<EventGroupType> type() {
    return EventGroupType.TYPE;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {

    /** The key value pairs defining the group. */
    @JsonProperty
    private Map<String, String> group;

    /** The latest event for this group. */
    @JsonProperty
    private EventsValue event;

    /**
     *
     * @param group Group defining the current group
     * @return The builder.
     */
    public Builder setGroup(Map<String, String> group) {
      this.group = group;
      return this;
    }

    /**
     *
     * @param event The latest event for the group
     * @return The builder.
     */
    public Builder setEvent(EventsValue event) {
      this.event = event;
      return this;
    }

    public EventsGroupValue build() {
      return new EventsGroupValue(this);
    }
  }

}
