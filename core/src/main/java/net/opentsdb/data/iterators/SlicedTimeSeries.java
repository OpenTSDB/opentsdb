// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A logical view on top of one or more time series iterators. For example, if
 * a query for 24 hours is split into 1 hour chunks with individual cache
 * entries for each hour, the hours can be re-merged into a logical view using
 * this class. 
 * <p>
 * Adding data is done via {@link #addSource(TimeSeries)}.
 * <ol>
 * <li>The first timestamp of the inserting source must be equal to or
 * greater than the final timestamp of the previous source.</li>
 * </ol>
 * <b>Note:</b> The order of chunks is not validated (since some of them may
 * come from cache).
 * 
 * TODO - need to perform validation
 * 
 * @since 3.0
 */
public class SlicedTimeSeries implements TimeSeries {

  /** The list of sources to combine into a logical view. */
  private List<TimeSeries> sources;
  
  /** The types of data available. */
  private Set<TypeToken<?>> types;
  
  /**
   * Default ctor. Sets up an array and instantiates a timestamp.
   */
  public SlicedTimeSeries() {
    sources = Lists.newArrayListWithExpectedSize(1);
    types = Sets.newHashSet();
  }
  
  /**
   * Adds a source to the set in order. Must have the same ID as the previous
   * sources and a greater or equivalent start timestamp to the end timestamp
   * of the previous iterator. 
   * <b>Note:</b> The first iterator inserted sets the ID for the set.
   * @param source A non-null source to add.
   */
  public void addSource(final TimeSeries source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    
    // TODO - validate
    sources.add(source);
    types.addAll(source.types());
  }
  
  @Override
  public TimeSeriesId id() {
    return sources.isEmpty() ? null : sources.get(0).id();
  }
  
  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
    iterator(final TypeToken<?> type) {
    if (!types.contains(type)) {
      return Optional.empty();
    }
    
    return Optional.of(new LocalIterator(type));
  }
  
  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithCapacity(types.size());
    for (final TypeToken<?> type : types) {
      iterators.add(new LocalIterator(type));
    }
    return iterators;
  }
  
  @Override
  public Collection<TypeToken<?>> types() {
    return Collections.unmodifiableSet(types);
  }
  
  @Override
  public void close() {
    for (final TimeSeries ts : sources) {
      ts.close();
    }
  }
  
  /**
   * An iterator over the sources, presenting a single logical view.
   *
   * @param <T> The type of data this iterator works over.
   */
  class LocalIterator<T extends TimeSeriesDataType> implements 
    Iterator<TimeSeriesValue<T>> {
    
    private final TypeToken<?> type;
    private int source_idx;
    private Iterator<TimeSeriesValue<?>> iterator;
    
    LocalIterator(final TypeToken<?> type) {
      this.type = type;
      for (source_idx = 0; source_idx < sources.size(); source_idx++) {
        final Optional<Iterator<TimeSeriesValue<?>>> it = 
            sources.get(source_idx).iterator(type);
        if (it.isPresent()) {
          iterator = it.get();
          break;
        }
      }
    }
    
    
    @Override
    public boolean hasNext() {
      if (source_idx >= sources.size()) {
        return false;
      }
      if (iterator != null && iterator.hasNext()) {
        return true;
      }
      
      source_idx++;
      for (; source_idx < sources.size(); source_idx++) {
        final Optional<Iterator<TimeSeriesValue<?>>> it = 
            sources.get(source_idx).iterator(type);
        if (it.isPresent()) {
          iterator = it.get();
          if (iterator.hasNext()) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public TimeSeriesValue<T> next() {
      return (TimeSeriesValue<T>) iterator.next();
    }
    
  }
  
}
