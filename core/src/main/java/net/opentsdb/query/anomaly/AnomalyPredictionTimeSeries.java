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

import java.io.IOException;
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
import net.opentsdb.data.types.numeric.aggregators.ArrayAggregatorUtils;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;

/**
 * A time series used to store the predictions generated off a baseline via a 
 * time series model. It can be copied from a cached result too.
 * 
 * When alerts are processed and found, they're added to this prediction time 
 * series.
 * 
 * @since 3.0
 */
public class AnomalyPredictionTimeSeries implements TimeSeries {
  protected final TimeSeriesId id;
  protected final TimeStamp timestamp;
  protected final NumericArrayAggregator aggregator;
  protected List<AlertValue> alerts;

  /**
   * CTor used when instantiating new predictions that have been computed
   * from a baseline or if the full prediction is already known.
   * @param id The non-null ID for the prediction series.
   * @param aggregator The optional aggregator. If null or empty then the
   *                   returned iterators will always return false on
   *                   {@link TypedTimeSeriesIterator#hasNext()}.
   * @param timestamp The non-null timestamp.
   */
  public AnomalyPredictionTimeSeries(final TimeSeriesId id,
                                     final NumericArrayAggregator aggregator,
                                     final TimeStamp timestamp) {
    this.id = id;
    this.aggregator = aggregator;
    this.timestamp = timestamp;
  }

  /**
   * Ctor used when joining cached predictions that were split across time
   * segments and need to be presented as a "unified" result. Overlaps are
   * accounted for. Each series MUST support the {@link NumericArrayType} and
   * the timestamp for each must be correct. That timestamp is used to accumulate
   * into the proper index of the agg array.
   *
   * @param sources A non-null array of one or more time series to accumulate.
   * @param node The non-null base node used to fetch the aggregator and model
   *             ID.
   * @param result The non-null anomaly result that will hold this time series.
   *               It is used to index into the aggregator properly.
   * @param suffix The suffix to append to the metric name in the {@link TimeSeriesId}.
   */
  public AnomalyPredictionTimeSeries(final TimeSeries[] sources,
                                     final BaseAnomalyNode node,
                                     final AnomalyQueryResult result,
                                     final String suffix) {
    this.aggregator = node.newAggregator();
    // TODO - handle byte IDs somehow.
    TimeSeriesStringId string_id = null;
    timestamp = result.timeSpecification().start();
    for (int i = 0; i < sources.length; i++) {
      if (sources[i] == null) {
        continue;
      }

      string_id = (TimeSeriesStringId) sources[i].id();
      break;
    }

    for (int i = 0; i < sources.length; i++) {
      final ArrayAggregatorUtils.AccumulateState state =
              ArrayAggregatorUtils.accumulateInAggregatorArray(aggregator,
                      result.timeSpecification().start(),
                      result.timeSpecification().end(),
                      result.timeSpecification().interval(),
                      sources[i]);
      if (state != ArrayAggregatorUtils.AccumulateState.SUCCESS) {
        // TODO - what should we do?
      }
    }
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
    tags.put(AnomalyTimeSeries.MODEL_TAG_KEY, node.modelId());
    builder.setTags(tags);
    this.id = builder.build();
  }

  /**
   * Replaces the reference to an alerts list.
   * <b>NOTE:</b> The list must be sorted in ascending time order.
   * @param results A populated alerts list or null to remove any alerts
   *                associated with this time series.
   */
  public void addAlerts(final List<AlertValue> results) {
    alerts = results;
  }

  /**
   * Adds an alert to the list. <b>NOTE:</b> no sorting is performed so please
   * add in the proper order. Also, if {@link #addAlerts(List)} was called, this
   * WILL mutate the original list.
   * @param alert A non-null alert to add.
   */
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
      its.add(new ArrayIterator());
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
    if (aggregator != null) {
      try {
        aggregator.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
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
    
    @Override
    public void close() {
      // no-op for now
    }
    
  }
  
  class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType> {

    boolean flipflop = (aggregator != null && aggregator.end() > aggregator.offset()) ? true : false;
    
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
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
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
      return aggregator;
    }
    
  }
  
}