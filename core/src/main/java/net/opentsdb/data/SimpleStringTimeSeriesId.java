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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
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
public class SimpleStringTimeSeriesId implements TimeSeriesId, Comparable<TimeSeriesId> {

  private byte[] alias;
  private List<byte[]> namespaces;
  private List<byte[]> metrics;
  private ByteMap<byte[]> tags;
  private List<byte[]> aggregated_tags;
  private List<byte[]> disjoint_tags;
  private ByteSet unique_ids;
  
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
      namespaces = Lists.newArrayListWithCapacity(builder.namespaces.size());
      for (final String namespace : builder.namespaces) {
        namespaces.add(namespace.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.metrics != null && !builder.metrics.isEmpty()) {
      metrics = Lists.newArrayListWithCapacity(builder.metrics.size());
      for (final String metric : builder.metrics) {
        metrics.add(metric.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.tags != null && !builder.tags.isEmpty()) {
      tags = new ByteMap<byte[]>();
      for (final Entry<String, String> pair : builder.tags.entrySet()) {
        tags.put(pair.getKey().getBytes(Const.UTF8_CHARSET), 
            pair.getValue().getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.aggregated_tags != null && !builder.aggregated_tags.isEmpty()) {
      aggregated_tags = Lists.newArrayListWithCapacity(builder.aggregated_tags.size());
      for (final String tag : builder.aggregated_tags) {
        aggregated_tags.add(tag.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.disjoint_tags != null && !builder.disjoint_tags.isEmpty()) {
      disjoint_tags = Lists.newArrayListWithCapacity(builder.disjoint_tags.size());
      for (final String tag : builder.disjoint_tags) {
        disjoint_tags.add(tag.getBytes(Const.UTF8_CHARSET));
      }
    }
    if (builder.unique_ids != null) {
      unique_ids = builder.unique_ids;
    }
  }
  
  @Override
  public boolean encoded() {
    // TODO
    return false;
  }

  @Override
  public byte[] alias() {
    return alias;
  }

  @Override
  public List<byte[]> namespaces() {
    return namespaces;
  }

  @Override
  public List<byte[]> metrics() {
    return metrics;
  }

  @Override
  public ByteMap<byte[]> tags() {
    return tags;
  }

  @Override
  public List<byte[]> aggregatedTags() {
    return aggregated_tags;
  }

  @Override
  public List<byte[]> disjointTags() {
    return disjoint_tags;
  }
  
  @Override
  public ByteSet uniqueIds() {
    return unique_ids;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    
    final TimeSeriesId id = (TimeSeriesId) o;
    
    // long slog through byte arrays.... :(
    if (Bytes.memcmpMaybeNull(alias, id.alias()) != 0) {
      return false;
    }
    if (!Bytes.equals(namespaces, id.namespaces())) {
      return false;
    }
    if (!Bytes.equals(metrics, id.metrics())) {
      return false;
    }
    if (!Bytes.equals(tags, id.tags())) {
      return false;
    }
    if (!Bytes.equals(aggregated_tags, id.aggregatedTags())) {
      return false;
    }
    if (!Bytes.equals(disjoint_tags, id.disjointTags())) {
      return false;
    }
    if (unique_ids != null && id.uniqueIds() == null) {
      return false;
    }
    if (unique_ids == null && id.uniqueIds() != null) {
      return false;
    }
    if (unique_ids != null && id.uniqueIds() != null) {
      return unique_ids.equals(id.uniqueIds());
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putBytes(alias)
        .putObject(namespaces, Bytes.BYTE_LIST_FUNNEL)
        .putObject(metrics, Bytes.BYTE_LIST_FUNNEL)
        .putObject(tags, Bytes.BYTE_MAP_FUNNEL)
        .putObject(aggregated_tags, Bytes.BYTE_LIST_FUNNEL)
        .putObject(disjoint_tags, Bytes.BYTE_LIST_FUNNEL)
        .putObject(unique_ids, ByteSet.BYTE_SET_FUNNEL)
        .hash();
  }
  
  @Override
  public int compareTo(final TimeSeriesId o) {
    return ComparisonChain.start()
        .compare(alias, o.alias(), Bytes.MEMCMP)
        .compare(namespaces, o.namespaces(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(metrics, o.metrics(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(tags, o.tags(), Bytes.BYTE_MAP_CMP)
        .compare(aggregated_tags, o.aggregatedTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(disjoint_tags, o.disjointTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(unique_ids, o.uniqueIds(), ByteSet.BYTE_SET_CMP)
        .result();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(new String(alias, Const.UTF8_CHARSET))
        .append(", namespaces=")
        .append(Bytes.toString(namespaces, Const.UTF8_CHARSET))
        .append(", metrics=")
        .append(Bytes.toString(metrics, Const.UTF8_CHARSET))
        .append(", tags=")
        .append(Bytes.toString(tags, Const.UTF8_CHARSET, Const.UTF8_CHARSET))
        .append(", aggregated_tags=")
        .append(Bytes.toString(aggregated_tags, Const.UTF8_CHARSET))
        .append(", disjoint_tags=")
        .append(Bytes.toString(disjoint_tags, Const.UTF8_CHARSET))
        .append(", uniqueIds=")
        .append(unique_ids);
    return buf.toString();
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
    
    public Builder setNamespaces(final List<String> namespace) {
      this.namespaces = namespace;
      if (this.namespaces != null) {
        Collections.sort(this.namespaces);
      }
      return this;
    }
    
    public Builder setMetrics(final List<String> metrics) {
      this.metrics = metrics;
      if (this.metrics != null) {
        Collections.sort(this.metrics);
      }
      return this;
    }
    
    public Builder setTags(final Map<String, String> tags) {
      this.tags = tags;
      return this;
    }
    
    public Builder setAggregatedTags(final List<String> aggregated_tags) {
      this.aggregated_tags = aggregated_tags;
      if (this.aggregated_tags != null) {
        Collections.sort(this.aggregated_tags);
      }
      return this;
    }
    
    public Builder setDisjointTags(final List<String> disjoint_tags) {
      this.disjoint_tags = disjoint_tags;
      if (this.disjoint_tags != null) {
        Collections.sort(this.disjoint_tags);
      }
      return this;
    }
    
    public Builder setUniqueId(final ByteSet unique_ids) {
      this.unique_ids = unique_ids;
      return this;
    }
    
    public Builder addUniqueId(final byte[] id) {
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
