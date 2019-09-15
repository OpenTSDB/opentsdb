// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * An iterator that handles combining multiple numeric type results from the 
 * cache into a single logical result.
 * 
 * @since 3.0
 */
public class CombinedCachedNumeric implements TypedTimeSeriesIterator<NumericType> {
  
  /** The array of source data. */
  private final TimeSeries[] series;
  
  /** The current index into the series. */
  private int idx = 0;
  
  /** The current iterator we're working on. */
  private TypedTimeSeriesIterator<NumericType> iterator;
  
  /**
   * Default ctor.
   * @param result The non-null combined result.
   * @param series The non-null result set.
   */
  CombinedCachedNumeric(final CombinedCachedResult result, 
                        final TimeSeries[] series) {
    this.series = series;
    while (series[idx] != null && idx < series.length) {
      iterator = (TypedTimeSeriesIterator<NumericType>) 
          series[idx].iterator(NumericType.TYPE).get();
      if (iterator.hasNext()) {
        break;
      }
      
      series[idx].close();
      iterator = null;
      idx++;
    }
  }

  @Override
  public boolean hasNext() {
    while (idx < series.length) {
      if (series[idx] == null) {
        idx++;
        continue;
      }
      
      if (iterator == null) {
        iterator = (TypedTimeSeriesIterator<NumericType>) 
            series[idx].iterator(NumericType.TYPE).get();
      }
      
      if (iterator.hasNext()) {
        return true;
      }
      
      series[idx].close();
      iterator = null;
      idx++;
    }
    return false;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    return iterator.next();
  }

  @Override
  public TypeToken<NumericType> getType() {
    return NumericType.TYPE;
  }

}
