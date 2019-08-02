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

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

public interface EventType extends TimeSeriesDataType<EventType> {

  /** The data type reference to pass around. */
  public static final TypeToken<EventType> TYPE = TypeToken.of(EventType.class);

  /** Source of the event, could be an agent emitting */
  public String source();

  /** Event title */
  public String title();

  /** Event content */
  public String message();

  /** Priority of the event, low/high/medium etc */
  public String priority();

  /** Timestamp of the event trigger */
  public TimeStamp timestamp();

  /** Timestamp of when the event has ended/will end */
  public TimeStamp endTimestamp();

  /** User of the event */
  public String userId();

  /** Whether the event is still on going */
  public boolean ongoing();

  /** Unique ID of the event */
  public String eventId();

  /** List of parent IDs this event can map to */
  public List<String> parentIds();

  /** List of child IDs this event can map to */
  public List<String> childIds();

  /** Additional properties */
  public Map<String, Object> additionalProps();

  /** Namespace if necessary */
  public String namespace();

}
