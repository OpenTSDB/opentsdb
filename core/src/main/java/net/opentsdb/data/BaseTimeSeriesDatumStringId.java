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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * A basic {@link TimeSeriesDatumStringId} implementation that accepts 
 * strings for all parameters. Includes a useful builder and after 
 * building, all lists are immutable.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = BaseTimeSeriesDatumStringId.Builder.class)
public class BaseTimeSeriesDatumStringId implements TimeSeriesDatumStringId {
  
  public static final MapComparator<String, String> STR_MAP_CMP = 
      new MapComparator<String, String>();
  
  /** An optional namespace. */
  protected String namespace;
  
  /** The required non-null and non-empty metric name. */
  protected String metric;
  
  /** A map of tag key/value pairs for the ID. */
  protected Map<String, String> tags;
  
  /** A cached hash code ID. */
  protected volatile long cached_hash; 
  
  /**
   * Private CTor used by the builder. Converts the Strings to byte arrays
   * using UTF8.
   * @param builder A non-null builder.
   */
  private BaseTimeSeriesDatumStringId(final Builder builder) {
    namespace = builder.namespace;
    metric = builder.metric;
    if (Strings.isNullOrEmpty(metric)) {
      throw new IllegalArgumentException("Metric cannot be null or empty.");
    }
    if (builder.tags != null && !builder.tags.isEmpty()) {
      for (final Entry<String, String> pair : builder.tags.entrySet()) {
        if (pair.getKey() == null) {
          throw new IllegalArgumentException("Tag key cannot be null.");
        }
        if (pair.getValue() == null) {
          throw new IllegalArgumentException("Tag value cannot be null.");
        }
        tags = Collections.unmodifiableMap(builder.tags);
      }
    } else {
      tags = Collections.emptyMap();
    }
  }

  @Override
  public String namespace() {
    return namespace;
  }

  @Override
  public String metric() {
    return metric;
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }

  @Override
  public int compareTo(final TimeSeriesDatumId o) {
    if (!(o instanceof TimeSeriesDatumStringId)) {
      return -1;
    }
    return ComparisonChain.start()
        .compare(Strings.nullToEmpty(namespace), 
            Strings.nullToEmpty(((TimeSeriesDatumStringId) o).namespace()))
        .compare(metric, ((TimeSeriesDatumStringId) o).metric())
        .compare(tags, ((TimeSeriesDatumStringId) o).tags(), STR_MAP_CMP)
        .result();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof TimeSeriesDatumStringId))
      return false;
    
    final TimeSeriesDatumStringId id = (TimeSeriesDatumStringId) o;
    if (!Objects.equal(namespace(), id.namespace())) {
      return false;
    }
    if (!Objects.equal(metric(), id.metric())) {
      return false;
    }
    if (!Objects.equal(tags(), id.tags())) {
      return false;
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    if (cached_hash == 0) {
      cached_hash = buildHashCode();
    }
    return Long.hashCode(cached_hash);
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public long buildHashCode() {
    final StringBuilder buf = new StringBuilder();
    buf.append(namespace);
    buf.append(metric);
    final TreeMap<String, String> sorted = 
        new TreeMap<String, String>(tags);
    if (tags != null) {
      for (final Entry<String, String> pair : sorted.entrySet()) {
        buf.append(pair.getKey());
        buf.append(pair.getValue());
      }
    }
    return LongHashFunction.xx_r39().hashChars(buf.toString());
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("namespace=")
        .append(namespace)
        .append(", metric=")
        .append(metric)
        .append(", tags=")
        .append(tags);
    return buf.toString();
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_STRING_ID;
  }
  
  /** @return A new builder or the SimpleStringTimeSeriesID. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String namespace;
    @JsonProperty
    private String metric;
    @JsonProperty
    private Map<String, String> tags;
    
    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }
    
    public Builder setMetric(final String metric) {
      this.metric = metric;
      return this;
    }
    
    /**
     * Sets the tags map. <b>NOTE:</b> This will maintain a reference to the
     * map and will NOT make a copy. Be sure to avoid mutating the map after 
     * passing it to the builder.
     * @param metrics A non-null list of metrics.
     * @return The builder object.
     */
    public Builder setTags(final Map<String, String> tags) {
      this.tags = tags;
      return this;
    }
    
    public Builder addTags(final String key, final String value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
      return this;
    }
    
    public BaseTimeSeriesDatumStringId build() {
      return new BaseTimeSeriesDatumStringId(this);
    }
  }

}
