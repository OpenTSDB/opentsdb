// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.TimeSeriesDatumIdPB;

/**
 * A protobuf converter for {@link TimeSeriesDatumStringId}s.
 * 
 * @since 3.0
 */
public class PBufTimeSeriesDatumId implements TimeSeriesDatumStringId {

  /** The source ID. */
  private TimeSeriesDatumIdPB.TimeSeriesDatumId id;
  
  /**
   * Protected ctor from the builder.
   * @param builder A non-null builder.
   */
  protected PBufTimeSeriesDatumId(final Builder builder) {
    id = builder.builder.build();
  }
  
  /**
   * Alternate ctor from another ID pbuf.
   * @param id A non-null pbuf.
   */
  protected PBufTimeSeriesDatumId(
      final TimeSeriesDatumIdPB.TimeSeriesDatumId id) {
    this.id = id;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_BYTE_ID;
  }

  @Override
  public long buildHashCode() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int compareTo(TimeSeriesDatumId o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public String namespace() {
    return id.getNamespace();
  }

  @Override
  public String metric() {
    return id.getMetric();
  }

  @Override
  public Map<String, String> tags() {
    return id.getTagsMap();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("namespace=")
        .append(namespace())
        .append(", metric=")
        .append(metric())
        .append(", tags=")
        .append(tags());
    return buf.toString();
  }
  
  public TimeSeriesDatumIdPB.TimeSeriesDatumId pbufID() {
    return id;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static Builder newBuilder(final TimeSeriesDatumId id) {
    if (!(id instanceof TimeSeriesDatumStringId)) {
      throw new IllegalArgumentException("ID must be of the type TimeSeriesDatumStringId");
    }
    
    final TimeSeriesDatumStringId string_id = (TimeSeriesDatumStringId) id;
    Builder builder = new Builder();
    if (string_id.namespace() != null) {
      builder.setNamespace(string_id.namespace());
    }
    if (string_id.metric() != null) {
      builder.setMetric(string_id.metric());
    }
    if (string_id.tags() != null) {
      builder.setTags(string_id.tags());
    }
    return builder;
  }
  
  public static final class Builder {
    private TimeSeriesDatumIdPB.TimeSeriesDatumId.Builder builder = 
        TimeSeriesDatumIdPB.TimeSeriesDatumId.newBuilder();
    
    public Builder setNamespace(final String namespace) {
      builder.setNamespace(namespace);
      return this;
    }
    
    public Builder setMetric(final String metric) {
      builder.setMetric(metric);
      return this;
    }
    
    public Builder setTags(final Map<String, String> tags) {
      builder.putAllTags(tags);
      return this;
    }
    
    public Builder addTags(final String key, final String value) {
      builder.putTags(key, value);
      return this;
    }
    
    public PBufTimeSeriesDatumId build() {
      return new PBufTimeSeriesDatumId(this);
    }
  }
}
