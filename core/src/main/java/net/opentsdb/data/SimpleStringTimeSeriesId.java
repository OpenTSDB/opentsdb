// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A basic {@link TimeSeriesId} implementation that accepts strings for all
 * parameters and encodes them using UTF8.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = SimpleStringTimeSeriesId.Builder.class)
public class SimpleStringTimeSeriesId extends TimeSeriesId {
  
  /**
   * Private CTor used by the builder. Converts the Strings to byte arrays
   * using UTF8.
   * @param builder A non-null builder.
   */
  private SimpleStringTimeSeriesId(final Builder builder) {
    if (!Strings.isNullOrEmpty(builder.alias)) {
      alias = builder.alias.getBytes(Const.UTF8_CHARSET);
    }
    if (builder.namespaces != null && !builder.namespaces.isEmpty()) {
      try {
        Collections.sort(builder.namespaces);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Namespaces cannot contain nulls");
      }
      namespaces = Lists.newArrayListWithCapacity(builder.namespaces.size());
      for (final String namespace : builder.namespaces) {
        namespaces.add(namespace.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.metrics != null && !builder.metrics.isEmpty()) {
      try {
        Collections.sort(builder.metrics);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Metrics cannot contain nulls");
      }
      metrics = Lists.newArrayListWithCapacity(builder.metrics.size());
      for (final String metric : builder.metrics) {
        metrics.add(metric.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.tags != null && !builder.tags.isEmpty()) {
      tags = new ByteMap<byte[]>();
      for (final Entry<String, String> pair : builder.tags.entrySet()) {
        if (pair.getKey() == null) {
          throw new IllegalArgumentException("Tag key cannot be null.");
        }
        if (pair.getValue() == null) {
          throw new IllegalArgumentException("Tag value cannot be null.");
        }
        tags.put(pair.getKey().getBytes(Const.UTF8_CHARSET), 
            pair.getValue().getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.aggregated_tags != null && !builder.aggregated_tags.isEmpty()) {
      try {
        Collections.sort(builder.aggregated_tags);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Aggregated tags cannot contain nulls");
      }
      aggregated_tags = Lists.newArrayListWithCapacity(builder.aggregated_tags.size());
      for (final String tag : builder.aggregated_tags) {
        aggregated_tags.add(tag.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.disjoint_tags != null && !builder.disjoint_tags.isEmpty()) {
      try {
        Collections.sort(builder.disjoint_tags);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Disjoint Tags cannot contain nulls");
      }
      disjoint_tags = Lists.newArrayListWithCapacity(builder.disjoint_tags.size());
      for (final String tag : builder.disjoint_tags) {
        disjoint_tags.add(tag.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.unique_ids != null) {
      unique_ids = builder.unique_ids;
    }
  }
  
  /** @return A new builder or the SimpleStringTimeSeriesID. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String alias;
    @JsonProperty
    private List<String> namespaces;
    @JsonProperty
    private List<String> metrics;
    @JsonProperty
    private Map<String, String> tags;
    @JsonProperty
    private List<String> aggregated_tags;
    @JsonProperty
    private List<String> disjoint_tags;
    @JsonProperty
    private ByteSet unique_ids; 
    
    public Builder setAlias(final String alias) {
      this.alias = alias;
      return this;
    }
    
    public Builder setNamespaces(final List<String> namespaces) {
      this.namespaces = namespaces;
      return this;
    }
    
    public Builder addNamespace(final String namespace) {
      if (namespaces == null) {
        namespaces = Lists.newArrayList();
      }
      namespaces.add(namespace);
      return this;
    }
    
    public Builder setMetrics(final List<String> metrics) {
      this.metrics = metrics;
      return this;
    }
    
    public Builder addMetric(final String metric) {
      if (metrics == null) {
        metrics = Lists.newArrayList();
      }
      metrics.add(metric);
      return this;
    }
    
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
    
    public Builder setAggregatedTags(final List<String> aggregated_tags) {
      this.aggregated_tags = aggregated_tags;
      return this;
    }
    
    public Builder addAggregatedTag(final String tag) {
      if (aggregated_tags == null) {
        aggregated_tags = Lists.newArrayList();
      }
      aggregated_tags.add(tag);
      return this;
    }
    
    public Builder setDisjointTags(final List<String> disjoint_tags) {
      this.disjoint_tags = disjoint_tags;
      return this;
    }
    
    public Builder addDisjointTag(final String tag) {
      if (disjoint_tags == null) {
        disjoint_tags = Lists.newArrayList();
      }
      disjoint_tags.add(tag);
      return this;
    }
    
    public Builder setUniqueId(final ByteSet unique_ids) {
      this.unique_ids = unique_ids;
      return this;
    }
    
    public Builder addUniqueId(final byte[] id) {
      if (id == null) {
        throw new IllegalArgumentException("Null unique IDs are not allowed.");
      }
      if (unique_ids == null) {
        unique_ids = new ByteSet();
      }
      unique_ids.add(id);
      return this;
    }
    
    public SimpleStringTimeSeriesId build() {
      return new SimpleStringTimeSeriesId(this);
    }
  }

}
