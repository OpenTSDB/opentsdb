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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import net.opentsdb.data.TimeStamp.Op;

/**
 * An interface used for efficient packing of time series that share the
 * same tags but have different metric names. The iterator should provide
 * a materialized view over the source data.
 * 
 * @since 3.0
 */
public interface TimeSeriesSharedTagsAndTimeData extends Iterable<TimeSeriesDatum> {

  /** @return The non-null timestamp shared across all data. */
  public TimeStamp timestamp();
  
  /** @return The non-null (possibly empty) set of tags common across all
   * data. */
  public Map<String, String> tags();
  
  /** @return A non-null and non-empty multi-map of metric names to values. 
   * Note that it's a multi-map in that a single metric may have multiple
   * data types associated with it in the same set. */
  public Multimap<String, TimeSeriesDataType> data();
  
  /**
   * A wrapper around a collection to create an iterable.
   * <b>NOTES:</b> All datum in the data must share the same timestamp 
   * and tag set. If any are different, an error is thrown. Likewise every
   * datum must have a non-null TimeSeriesValue, however the value of that
   * value may be null.
   * 
   * @param data The non-null data to read from.
   * @return The iterable collection.
   * @throws IllegalArgumentException if the collection was null or empty
   * or a requirement wasn't satisfied.
   */
  public static TimeSeriesSharedTagsAndTimeData fromCollection(
      final Collection<TimeSeriesDatum> data) {
    if (data == null || data.isEmpty()) {
      throw new IllegalArgumentException("Datum collection cannot be "
          + "null or empty.");
    }
    
    final TimeSeriesDatum example = data.iterator().next();
    if (example.value() == null) {
      throw new IllegalArgumentException("Datum value can't be null.");
    }
    
    // validation
    for (final TimeSeriesDatum dp : data) {
      if (dp == example) {
        continue;
      }
      
      if (!((TimeSeriesDatumStringId) dp.id()).tags().equals(
          ((TimeSeriesDatumStringId) example.id()).tags())) {
        throw new IllegalArgumentException("Tags differed in the collection.");
      }
      
      if (dp.value() == null) {
        throw new IllegalArgumentException("Datum value can't be null.");
      }
      
      if (dp.value().timestamp().compare(Op.NE, example.value().timestamp())) {
        throw new IllegalArgumentException("Timestamps differed in the collection.");
      }
    }
    
    return new TimeSeriesSharedTagsAndTimeData() {
      
      @Override
      public TimeStamp timestamp() {
        return example.value().timestamp();
      }
      
      @Override
      public Map<String, String> tags() {
        return ((TimeSeriesDatumStringId) example.id()).tags();
      }
      
      @Override
      public Multimap<String, TimeSeriesDataType> data() {
        final Multimap<String, TimeSeriesDataType> map = 
            ArrayListMultimap.create(data.size(), 1);
        for (final TimeSeriesDatum dp : data) {
          map.put(((TimeSeriesDatumStringId) dp.id()).metric(), 
              dp.value().value());
        }
        return map;
      }
      
      @Override
      public Iterator<TimeSeriesDatum> iterator() {
        return data.iterator();
      }
    };
  }
}
