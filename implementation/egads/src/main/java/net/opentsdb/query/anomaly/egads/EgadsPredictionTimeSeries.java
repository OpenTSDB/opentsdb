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
package net.opentsdb.query.anomaly.egads;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayType;

/**
 * A time series used to store the predictions generated off a baseline via a 
 * time series model. It can be copied from a cached result too.
 * 
 * When alerts are processed and found, they're added to this prediction time 
 * series.
 * 
 * @since 3.0
 */
public class EgadsPredictionTimeSeries implements TimeSeries {
  protected final TimeSeries source;
  protected final TimeSeriesId id;
  protected final TimeStamp timestamp;
  protected final double[] results;
  protected List<AlertValue> alerts;
  
  public EgadsPredictionTimeSeries(final TimeSeriesId id, 
                                   final double[] results, 
                                   final TimeStamp timestamp) {
    this.source = null;
    this.id = id;
    this.results = results;
    this.timestamp = timestamp;
  }
  
  public EgadsPredictionTimeSeries(final TimeSeries source, 
                                   final String suffix, 
                                   final String model) {
    this.source = source;
    // TODO - handle byte IDs somehow.
    final TimeSeriesStringId string_id = (TimeSeriesStringId) source.id();
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
    tags.put(EgadsTimeSeries.MODEL_TAG_KEY, model);
    builder.setTags(tags);
    this.id = builder.build();
    this.results = null;
    this.timestamp = null;
  }
  
  public void addAlerts(final List<AlertValue> results) {
    alerts = results;
  }
  
  public void addAlert(final AlertValue alert) {
    if (alerts == null) {
      alerts = Lists.newArrayList();
    }
    alerts.add(alert);
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type == NumericArrayType.TYPE) {
      if (source != null) {
        return source.iterator(type);
      }
      return Optional.of(new ArrayIterator());
    } else if (type == AlertType.TYPE && alerts != null && !alerts.isEmpty()) {
      return Optional.of(new AlertIterator());
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
        Lists.newArrayList();
    if (source != null) {
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
          source.iterator(NumericArrayType.TYPE);
      if (op.isPresent()) {
        its.add(op.get());
      }
    } else {
      its.add(new ArrayIterator());
    }
    if (alerts != null && !alerts.isEmpty()) {
      its.add(new AlertIterator());
    }
    return its;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    if (alerts == null || alerts.isEmpty()) {
      return Sets.newHashSet(NumericArrayType.TYPE);
    } else {
      return Sets.newHashSet(NumericArrayType.TYPE, AlertType.TYPE);
    }
  }

  @Override
  public void close() {
    if (source != null) {
      source.close();
    }
  }
  
  class AlertIterator implements TypedTimeSeriesIterator<AlertType> {
    int idx = 0;
    
    @Override
    public boolean hasNext() {
      return idx < alerts.size();
    }

    @Override
    public TimeSeriesValue<AlertType> next() {
      return alerts.get(idx++);
    }

    @Override
    public TypeToken<AlertType> getType() {
      return AlertType.TYPE;
    }
    
  }
  
  class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, NumericArrayType {

    boolean flipflop = (results != null && results.length > 0) ? true : false;
    
    @Override
    public boolean hasNext() {
      return flipflop;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      flipflop = false;
      return this;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public int end() {
      return results.length;
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
      return results;
    }

    @Override
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public NumericArrayType value() {
      return this;
    }
    
  }
  
}