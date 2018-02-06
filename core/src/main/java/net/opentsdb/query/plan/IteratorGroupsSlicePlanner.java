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
package net.opentsdb.query.plan;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
//import net.opentsdb.data.iterators.SlicedTimeSeriesIterator;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * Slice planner that generates the time splices on instantiation and deals with
 * {@link IteratorGroups} as the data type.
 * 
 * @since 3.0
 */
public class IteratorGroupsSlicePlanner extends 
    TimeSlicedQueryPlanner<IteratorGroups> {

  /**
   * Default ctor that initializes the slices 
   * @param query
   */
  public IteratorGroupsSlicePlanner(final TimeSeriesQuery query) {
    super(query);
    generatePlan();
  }

  @SuppressWarnings("unchecked")
  @Override
  public IteratorGroups mergeSlicedResults(final List<IteratorGroups> results) {
//    if (results == null) {
//      throw new IllegalArgumentException("Results cannot be null.");
//    }
//    // super ugly, ripe for optimization. May be a better way but we have to 
//    // sort and add the chunks to the proper slice iterator then feed them into 
//    // groups.
//    final Map<TimeSeriesGroupId, Map<TimeSeriesId, 
//    SlicedTimeSeriesIterator<NumericType>>> its = Maps.newHashMap();
//    final IteratorGroups merged = new DefaultIteratorGroups();
//    
//    for (final IteratorGroups result : results) {
//      if (result == null) {
//        continue;
//      }
//      final IteratorGroups groups = (IteratorGroups) result;
//      for (final Entry<TimeSeriesGroupId, IteratorGroup> entry : groups) {
//        Map<TimeSeriesId, SlicedTimeSeriesIterator<NumericType>> series = 
//            its.get(entry.getKey());
//        if (series == null) {
//          series = Maps.newHashMap();
//          its.put(entry.getKey(), series);
//        }
//        for (final TimeSeriesIterator<?> iterator : 
//            entry.getValue().flattenedIterators()) {
//          SlicedTimeSeriesIterator<NumericType> slice = series.get(iterator.id());
//          if (slice == null) {
//            slice = new SlicedTimeSeriesIterator<NumericType>();
//            slice.addIterator((TimeSeriesIterator<NumericType>) iterator);
//            series.put(iterator.id(), slice);
//            merged.addIterator(entry.getKey(), slice);
//          } else {
//            slice.addIterator((TimeSeriesIterator<NumericType>) iterator);
//          }
//        }
//      }
//    }
//    return merged;
    return null;
  }

  @Override
  public List<IteratorGroups> sliceResult(final IteratorGroups result_copy,
                                          final int start_index, 
                                          final int end_index) {
    if (result_copy == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (start_index < 0) {
      throw new IllegalArgumentException("Start index must be zero or greater.");
    }
    if (start_index >= query_time_ranges.length) {
      throw new IllegalArgumentException("Start index must be less than " 
          + query_time_ranges.length + ".");
    }
    if (start_index > end_index) {
      throw new IllegalArgumentException("Start index must be less than or "
          + "equal to the end index.");
    }
    if (end_index < 0) {
      throw new IllegalArgumentException("End index must be zero or greater.");
    }
    if (end_index >= query_time_ranges.length) {
      throw new IllegalArgumentException("Start index must be less than " 
          + query_time_ranges.length + ".");
    }
    if (start_index == end_index) {
      return Lists.<IteratorGroups>newArrayList(result_copy);
    }
    
    final List<IteratorGroups> results = 
        Lists.newArrayListWithCapacity(end_index - start_index);      
      for (int i = start_index; i <= end_index; i++) {
        final IteratorGroups for_cache = new DefaultIteratorGroups();
        for (final Entry<TimeSeriesGroupId, IteratorGroup> entry : result_copy) {
          for (final TimeSeriesIterator<?> iterator : 
            entry.getValue().flattenedIterators()) {
            for_cache.addIterator(entry.getKey(), 
                iterator.getDeepCopy(null, query_time_ranges[i][0], 
                    query_time_ranges[i][1]));
          }
        }
        results.add(for_cache);
      }
      return results;
  }

  @Override
  protected void generatePlan() {
    query_time_ranges = getTimeRanges(query);
  }
  
}
