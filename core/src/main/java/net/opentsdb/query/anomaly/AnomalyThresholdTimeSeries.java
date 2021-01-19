// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;

/**
 * An anomaly threshold computation time series (i.e. given the thresholds in a
 * config, computes them using the prediction.
 * 
 * @since 3.0
 */
public class AnomalyThresholdTimeSeries implements TimeSeries {

  private final TimeSeriesId id;
  private final TimeStamp timestamp;
  private final double[] values;
  private final int end_idx;
  
  public AnomalyThresholdTimeSeries(final TimeSeriesId id, 
                                  final String suffix,
                                  final long prediction_start,
                                  final double[] values, 
                                  final int end_idx,
                                  final String model) {
    // TODO - handle byte IDs somehow.
    final TimeSeriesStringId string_id = (TimeSeriesStringId) id;
    BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder()
        .setAlias(string_id.alias())
        .setNamespace(string_id.namespace())
        .setMetric(string_id.metric() + "." + suffix)
        .setAggregatedTags(string_id.aggregatedTags())
        .setDisjointTags(string_id.disjointTags());
    final Map<String, String> tags = Maps.newHashMap();
    if (string_id.tags() != null) {
      tags.putAll(string_id.tags());
    }
    tags.put(AnomalyTimeSeries.MODEL_TAG_KEY, model);
    builder.setTags(tags);
    this.id = builder.build();
    this.timestamp = new SecondTimeStamp(prediction_start);
    this.values = values;
    this.end_idx = end_idx;
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericArrayType.TYPE) {
      return Optional.empty();
    }
    return Optional.of(new ArrayIterator());
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
        Lists.newArrayListWithCapacity(1);
    its.add(new ArrayIterator());
    return its;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return Sets.newHashSet(NumericArrayType.TYPE);
  }

  @Override
  public void close() {
    // no-op
  }
  
  class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
      TimeSeriesValue<NumericArrayType>,
      NumericArrayType {

    private boolean has_next = true;
    
    @Override
    public boolean hasNext() {
      return has_next;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      has_next = false;
      return this;
    }

    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public NumericArrayType value() {
      return this;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
    }
    
    @Override
    public int offset() {
      return 0;
    }
    
    @Override
    public int end() {
      return end_idx;
    }
    
    @Override
    public boolean isInteger() {
      return false;
    }
    
    @Override
    public long[] longArray() {
      return null;
    }
    
    @Override
    public double[] doubleArray() {
      return values;
    }
    
  }
}