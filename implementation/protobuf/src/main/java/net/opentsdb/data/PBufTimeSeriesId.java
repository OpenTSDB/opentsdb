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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.TimeSeriesIdPB;

/**
 * A protobuf converter for {@link TimeSeriesStringId}s.
 * 
 * @since 3.0
 */
public class PBufTimeSeriesId implements TimeSeriesStringId {

  /** The source ID. */
  private TimeSeriesIdPB.TimeSeriesId id;
  
  /** A cached hash code ID. */
  protected volatile long cached_hash; 
  
  /**
   * Protected ctor from the builder.
   * @param builder A non-null builder.
   */
  protected PBufTimeSeriesId(final Builder builder) {
    id = builder.builder.build();
  }
  
  /**
   * Alternate ctor from another ID pbuf.
   * @param id A non-null pbuf.
   */
  protected PBufTimeSeriesId(final TimeSeriesIdPB.TimeSeriesId id) {
    this.id = id;
  }
  
  @Override
  public boolean encoded() {
    return id.getEncoded();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_STRING_ID;
  }

  @Override
  public int compareTo(final TimeSeriesStringId o) {
    return ComparisonChain.start()
        .compare(Strings.nullToEmpty(id.getAlias()), Strings.nullToEmpty(o.alias()))
        .compare(Strings.nullToEmpty(id.getNamespace()), Strings.nullToEmpty(o.namespace()))
        .compare(id.getMetric(), o.metric())
        .compare(id.getTagsMap(), o.tags(), BaseTimeSeriesStringId.STR_MAP_CMP)
        .compare(id.getAggregatedTagsList(), o.aggregatedTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(id.getDisjointTagsList(), o.disjointTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(id.getUniqueIdsList(), o.uniqueIds(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof TimeSeriesStringId))
      return false;
    
    final TimeSeriesStringId id = (TimeSeriesStringId) o;
    
    if (!Objects.equal(alias(), id.alias())) {
      return false;
    }
    if (!Objects.equal(namespace(), id.namespace())) {
      return false;
    }
    if (!Objects.equal(metric(), id.metric())) {
      return false;
    }
    if (!Objects.equal(tags(), id.tags())) {
      return false;
    }
    if (!Objects.equal(aggregatedTags(), id.aggregatedTags())) {
      return false;
    }
    if (!Objects.equal(disjointTags(), id.disjointTags())) {
      return false;
    }
    if (!Objects.equal(uniqueIds(), id.uniqueIds())) {
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
  
  @Override
  public long buildHashCode() {
    final StringBuilder buf = new StringBuilder();
    if (alias() != null) {
      buf.append(alias());
    }
    buf.append(namespace());
    buf.append(metric());
    if (tags() != null) {
      for (final Entry<String, String> pair : tags().entrySet()) {
        buf.append(pair.getKey());
        buf.append(pair.getValue());
      }
    }
    if (aggregatedTags() != null) {
      for (final String t : aggregatedTags()) {
        buf.append(t);
      }
    }
    if (disjointTags() != null) {
      for (final String t : disjointTags()) {
        buf.append(t);
      }
    }
    if (uniqueIds() != null) {
      final List<String> sorted = Lists.newArrayList(uniqueIds());
      Collections.sort(sorted);
      for (final String id : sorted) {
        buf.append(id);
      }
    }
    return LongHashFunction.xx_r39().hashChars(buf.toString());
  }
  
  @Override
  public String alias() {
    return id.getAlias();
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
  public List<String> aggregatedTags() {
    return id.getAggregatedTagsList();
  }

  @Override
  public List<String> disjointTags() {
    return id.getDisjointTagsList();
  }

  @Override
  public Set<String> uniqueIds() {
    // TODO - cache.
    return Sets.newHashSet(id.getUniqueIdsList());
  }

  public TimeSeriesIdPB.TimeSeriesId pbufID() {
    return id;
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias() != null ? alias() : "null")
        .append(", namespace=")
        .append(namespace())
        .append(", metric=")
        .append(metric())
        .append(", tags=")
        .append(tags())
        .append(", aggregated_tags=")
        .append(aggregatedTags())
        .append(", disjoint_tags=")
        .append(disjointTags())
        .append(", uniqueIds=")
        .append(uniqueIds());
    return buf.toString();
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static Builder newBuilder(final TimeSeriesId id) {
    if (id.type() != Const.TS_STRING_ID) {
      throw new IllegalArgumentException("ID must be of the type: " 
          + Const.TS_STRING_ID);
    }
    
    final TimeSeriesStringId string_id = (TimeSeriesStringId) id;
    Builder builder = new Builder()
        .setEncoded(id.encoded());
    if (string_id.alias() != null) {
      builder.setAlias(string_id.alias());
    }
    if (string_id.namespace() != null) {
      builder.setNamespace(string_id.namespace());
    }
    if (string_id.metric() != null) {
      builder.setMetric(string_id.metric());
    }
    if (string_id.tags() != null) {
      builder.setTags(string_id.tags());
    }
    if (string_id.aggregatedTags() != null) {
      builder.setAggregatedTags(string_id.aggregatedTags());
    }
    if (string_id.disjointTags() != null) {
      builder.setDisjointTags(string_id.disjointTags());
    }
    if (string_id.uniqueIds() != null) {
      builder.setUniqueId(string_id.uniqueIds());
    }
    return builder;
  }
  
  public static final class Builder {
    private TimeSeriesIdPB.TimeSeriesId.Builder builder = 
        TimeSeriesIdPB.TimeSeriesId.newBuilder();
    
    public Builder setEncoded(final boolean encoded) {
      builder.setEncoded(encoded);
      return this;
    }
    
    public Builder setAlias(final String alias) {
      builder.setAlias(alias);
      return this;
    }
    
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
    
    public Builder setAggregatedTags(final List<String> aggregated_tags) {
      builder.addAllAggregatedTags(aggregated_tags);
      return this;
    }
    
    public Builder addAggregatedTag(final String tag) {
      builder.addAggregatedTags(tag);
      return this;
    }
    
    public Builder setDisjointTags(final List<String> disjoint_tags) {
      builder.addAllDisjointTags(disjoint_tags);
      return this;
    }
    
    public Builder addDisjointTag(final String tag) {
      builder.addDisjointTags(tag);
      return this;
    }
    
    public Builder setUniqueId(final Set<String> unique_ids) {
      builder.addAllUniqueIds(unique_ids);
      return this;
    }
    
    public Builder addUniqueId(final String id) {
      builder.addUniqueIds(id);
      return this;
    }
    
    public PBufTimeSeriesId build() {
      return new PBufTimeSeriesId(this);
    }
    
  }
}
