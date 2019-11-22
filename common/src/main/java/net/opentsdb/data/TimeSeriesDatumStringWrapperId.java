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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * A wrapper to convert a {@link TimeSeriesDatumStringId} to a 
 * {@link TimeSeriesStringId}.
 * 
 * @since 3.0
 */
public class TimeSeriesDatumStringWrapperId implements TimeSeriesStringId {

  /** A static comparator instantiation. */
  public static final MapComparator<String, String> STR_MAP_CMP = 
      new MapComparator<String, String>();

  /** The ID. */
  protected final TimeSeriesDatumStringId id;
 
  /**
   * Protected ctor. Call {@link #wrap(TimeSeriesDatumStringId)}.
   * @param id The non-null ID.
   */
  protected TimeSeriesDatumStringWrapperId(final TimeSeriesDatumStringId id) {
    this.id = id;
  }
  
  /**
   * Wraps the given ID.
   * @param id The non-null ID to wrap.
   * @return The wrapped ID.
   * @throws IllegalArgumentException if the ID was null.
   */
  public static TimeSeriesDatumStringWrapperId wrap(
      final TimeSeriesDatumStringId id) {
    if (id == null) {
      throw new IllegalArgumentException("The ID to wrap cannot be null.");
    }
    return new TimeSeriesDatumStringWrapperId(id);
  }

  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_STRING_ID;
  }

  @Override
  public int compareTo(final TimeSeriesStringId o) {
    return ComparisonChain.start()
        .compare(Strings.nullToEmpty(id.namespace()), 
            Strings.nullToEmpty(o.namespace()))
        .compare(id.metric(), o.metric())
        .compare(id.tags(), o.tags(), STR_MAP_CMP)
        .result();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof TimeSeriesStringId)) {
      return false;
    }
    
    final TimeSeriesStringId id = (TimeSeriesStringId) o;
    return Objects.equal(namespace(), id.namespace()) &&
           Objects.equal(metric(), id.metric()) &&
           Objects.equal(tags(), id.tags());
  }
  
  @Override
  public int hashCode() {
    return id.hashCode();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public long buildHashCode() {
    return id.buildHashCode();
  }
  
  @Override
  public String alias() {
    return null;
  }

  @Override
  public String namespace() {
    return id.namespace();
  }

  @Override
  public String metric() {
    return id.metric();
  }

  @Override
  public Map<String, String> tags() {
    return id.tags();
  }

  @Override
  public String getTagValue(String key) {
    return tags().get(key);
  }

  @Override
  public List<String> aggregatedTags() {
    return Collections.emptyList();
  }

  @Override
  public List<String> disjointTags() {
    return Collections.emptyList();
  }

  @Override
  public Set<String> uniqueIds() {
    // TODO Auto-generated method stub
    return Collections.emptySet();
  }

  @Override
  public long hits() {
    return 0;
  }
  
  @Override
  public String toString() {
    return id.toString();
  }
  
}
