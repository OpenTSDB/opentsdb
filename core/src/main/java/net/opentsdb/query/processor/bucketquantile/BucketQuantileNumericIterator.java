// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * Simple iterator that wraps up the quantiles array and returns it.
 * 
 * @since 3.0
 */
public class BucketQuantileNumericIterator extends BucketQuantileIterator  
  implements TimeSeries, 
             TypedTimeSeriesIterator<NumericType> {

  private final BucketQuantileNumericProcessor processor;
  private final MutableNumericValue dp;
  private int index;
  
  BucketQuantileNumericIterator(
      final int quantile_index,
      final BucketQuantileNumericProcessor processor) {
    super(quantile_index, processor);
    this.processor = processor;
    dp = new MutableNumericValue();
  }

  @Override
  public TypeToken getType() {
    return NumericType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return index < processor.limit;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    dp.timestamp().updateEpoch(processor.timestamps[index]);
    dp.resetValue(processor.quantiles[quantile_index][index]);
    index++;
    return dp;
  }
  
  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericType.TYPE) {
      return Optional.empty();
    }
    return Optional.of((TypedTimeSeriesIterator<? extends TimeSeriesDataType>) this);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = this;
    return Lists.<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>newArrayList(it);
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return NumericType.SINGLE_LIST;
  }

  @Override
  public void close() {
    // no-op
  }
  
}
