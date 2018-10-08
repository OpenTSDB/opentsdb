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
package net.opentsdb.query.joins;

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
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;

/**
 * A simple wrapper for single-sided joins that wraps the source 
 * ID with the proper alias for use as the alias and metric.
 * 
 * @since 3.0
 */
public class StringIdOverride implements TimeSeriesStringId {
  
  /** The source ID. */
  private final TimeSeriesStringId id;
  
  /** The new alias. */
  private final String alias;
  
  /** A cached hash code ID. */
  protected volatile long cached_hash; 
  
  /**
   * Default package private ctor.
   * @param id A non-null ID to use as the source.
   * @param alias A non-null alias.
   */
  StringIdOverride(final TimeSeriesStringId id, final String alias) {
    this.id = id;
    this.alias = alias;
  }
  
  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return id.type();
  }
  
  @Override
  public int compareTo(final TimeSeriesStringId o) {
    return ComparisonChain.start()
        .compare(Strings.nullToEmpty(alias), Strings.nullToEmpty(o.alias()))
        .compare(Strings.nullToEmpty(id.namespace()), Strings.nullToEmpty(o.namespace()))
        .compare(alias, o.metric())
        .compare(id.tags(), o.tags(), BaseTimeSeriesStringId.STR_MAP_CMP)
        .compare(id.aggregatedTags(), o.aggregatedTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(id.disjointTags(), o.disjointTags(), 
            Ordering.<String>natural().lexicographical().nullsFirst())
        .compare(id.uniqueIds(), o.uniqueIds(), 
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
    buf.append(id.namespace());
    buf.append(alias);
    if (id.tags() != null) {
      for (final Entry<String, String> pair : id.tags().entrySet()) {
        buf.append(pair.getKey());
        buf.append(pair.getValue());
      }
    }
    if (id.aggregatedTags() != null) {
      for (final String t : id.aggregatedTags()) {
        buf.append(t);
      }
    }
    if (id.disjointTags() != null) {
      for (final String t : id.disjointTags()) {
        buf.append(t);
      }
    }
    if (id.uniqueIds() != null) {
      final List<String> sorted = Lists.newArrayList(id.uniqueIds());
      Collections.sort(sorted);
      for (final String id : sorted) {
        buf.append(id);
      }
    }
    return LongHashFunction.xx_r39().hashChars(buf.toString());
  }
  
  @Override
  public String alias() {
    return alias;
  }

  @Override
  public String namespace() {
    return id.namespace();
  }

  @Override
  public String metric() {
    return alias;
  }

  @Override
  public Map<String, String> tags() {
    return id.tags();
  }

  @Override
  public List<String> aggregatedTags() {
    return id.aggregatedTags();
  }

  @Override
  public List<String> disjointTags() {
    return id.disjointTags();
  }

  @Override
  public Set<String> uniqueIds() {
    return id.uniqueIds();
  }
  
}