// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;

/**
 * A basic {@link TimeSeriesStringId} implementation that accepts strings for all
 * parameters. Includes a useful builder and after building, all lists are 
 * immutable.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = BaseTimeSeriesStringId.Builder.class)
public class BaseTimeSeriesStringId implements TimeSeriesStringId {

  /** Whether or not the strings are specially encoded values. */
  protected boolean encoded;
  
  /** An optional alias. */
  protected String alias;
  
  /** An optional namespace. */
  protected String namespace;
  
  /** The required non-null and non-empty metric name. */
  protected String metric;
  
  /** A map of tag key/value pairs for the ID. */
  protected Map<String, String> tags;
  
  /** An optional list of aggregated tags for the ID. */
  protected List<String> aggregated_tags;
  
  /** An optional list of disjoint tags for the ID. */
  protected List<String> disjoint_tags;
  
  /** A list of unique IDs rolled up into the ID. */
  protected Set<String> unique_ids;
  
  /** A cached hash code ID. */
  protected volatile long cached_hash; 
  
  /**
   * Private CTor used by the builder. Converts the Strings to byte arrays
   * using UTF8.
   * @param builder A non-null builder.
   */
  private BaseTimeSeriesStringId(final Builder builder) {
    encoded = builder.encoded;
    alias = builder.alias;
    namespace = builder.namespace;
    metric = builder.metric;
    if (Strings.isNullOrEmpty(metric)) {
      throw new IllegalArgumentException("Metric cannot be null or empty.");
    }
    if (builder.tags != null && !builder.tags.isEmpty()) {
      tags = builder.tags;
    } else {
      tags = Collections.emptyMap();
    }
    if (builder.aggregated_tags != null && !builder.aggregated_tags.isEmpty()) {
      try {
        Collections.sort(builder.aggregated_tags);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Aggregated tags cannot contain nulls");
      }
      aggregated_tags = builder.aggregated_tags;
    } else {
      aggregated_tags = Collections.emptyList();
    }
    if (builder.disjoint_tags != null && !builder.disjoint_tags.isEmpty()) {
      try {
        Collections.sort(builder.disjoint_tags);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Disjoint Tags cannot contain nulls");
      }
      disjoint_tags = builder.disjoint_tags;
    } else {
      disjoint_tags = Collections.emptyList();
    }
    if (builder.unique_ids != null) {
      unique_ids = builder.unique_ids;
    } else {
      unique_ids = Collections.emptySet();
    }
  }

  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public String alias() {
    return alias;
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
  public List<String> aggregatedTags() {
    return aggregated_tags;
  }

  @Override
  public List<String> disjointTags() {
    return disjoint_tags;
  }

  @Override
  public Set<String> uniqueIds() {
    return unique_ids;
  }

  @Override
  public int compareTo(final TimeSeriesStringId o) {
    return ComparisonChain.start()
        .compare(Strings.nullToEmpty(alias), Strings.nullToEmpty(o.alias()))
        .compare(Strings.nullToEmpty(namespace), Strings.nullToEmpty(o.namespace()))
        .compare(metric, o.metric())
        .compare(tags, o.tags(), STR_MAP_CMP)
        .compare(aggregated_tags, o.aggregatedTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(disjoint_tags, o.disjointTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(unique_ids, o.uniqueIds(), 
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
    
    if (!Objects.equal(alias, id.alias())) {
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
    if (alias != null) {
      buf.append(alias);
    }
    buf.append(namespace);
    buf.append(metric);
    if (tags != null) {
      for (final Entry<String, String> pair : tags.entrySet()) {
        buf.append(pair.getKey());
        buf.append(pair.getValue());
      }
    }
    if (aggregated_tags != null) {
      for (final String t : aggregated_tags) {
        buf.append(t);
      }
    }
    if (disjoint_tags != null) {
      for (final String t : disjoint_tags) {
        buf.append(t);
      }
    }
    if (unique_ids != null) {
      final List<String> sorted = Lists.newArrayList(unique_ids);
      Collections.sort(sorted);
      for (final String id : sorted) {
        buf.append(id);
      }
    }
    return LongHashFunction.xx_r39().hashChars(buf.toString());
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias != null ? alias : "null")
        .append(", namespace=")
        .append(namespace)
        .append(", metric=")
        .append(metric)
        .append(", tags=")
        .append(tags)
        .append(", aggregated_tags=")
        .append(aggregated_tags)
        .append(", disjoint_tags=")
        .append(disjoint_tags)
        .append(", uniqueIds=")
        .append(unique_ids);
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
    private boolean encoded;
    @JsonProperty
    private String alias;
    @JsonProperty
    private String namespace;
    @JsonProperty
    private String metric;
    @JsonProperty
    private Map<String, String> tags;
    @JsonProperty
    private List<String> aggregated_tags;
    @JsonProperty
    private List<String> disjoint_tags;
    @JsonProperty
    private Set<String> unique_ids; 
    
    public Builder setEncoded(final boolean encoded) {
      this.encoded = encoded;
      return this;
    }
    
    public Builder setAlias(final String alias) {
      this.alias = alias;
      return this;
    }
    
    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }
    
    public String namespace() {
      return namespace;
    }
    
    public Builder setMetric(final String metric) {
      this.metric = metric;
      return this;
    }
    
    public String metric() {
      return metric;
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
    
    public Map<String, String> tags() {
      return tags == null ? Collections.emptyMap() : tags;
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
    
    public List<String> aggregatedTags() {
      return aggregated_tags == null ? Collections.emptyList() : 
        aggregated_tags;
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
    
    public List<String> disjointTags() {
      return disjoint_tags == null ? Collections.emptyList() : 
        disjoint_tags;
    }
    
    public Builder setUniqueId(final Set<String> unique_ids) {
      this.unique_ids = unique_ids;
      return this;
    }
    
    public Builder addUniqueId(final String id) {
      if (id == null) {
        throw new IllegalArgumentException("Null unique IDs are not allowed.");
      }
      if (unique_ids == null) {
        unique_ids = new HashSet<String>();
      }
      unique_ids.add(id);
      return this;
    }
    
    public Set<String> uniqueIds() {
      return unique_ids == null ? Collections.emptySet() : unique_ids;
    }
    
    public BaseTimeSeriesStringId build() {
      return new BaseTimeSeriesStringId(this);
    }
  }

  /** A static comparator instantiation. */
  public static final StringMapComparator STR_MAP_CMP = new StringMapComparator();
  
  /**
   * A simple comparator for maps of strings.
   */
  static class StringMapComparator implements Comparator<Map<String, String>> {
    private StringMapComparator() { }
    @Override
    public int compare(final Map<String, String> a, final Map<String, String> b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final Entry<String, String> entry : a.entrySet()) {
        final String b_value = b.get(entry.getKey());
        if (b_value == null && entry.getValue() != null) {
          return 1;
        }
        final int cmp = entry.getValue().compareTo(b_value);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
  }

}
