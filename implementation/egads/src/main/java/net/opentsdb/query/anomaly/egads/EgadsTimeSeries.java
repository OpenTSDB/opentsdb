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
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TypedTimeSeriesIterator;

/**
 * A time series class that simply tweaks the ID, adding a suffix string to
 * the metric name and a tag for the given model to the tag set.
 */
public class EgadsTimeSeries implements TimeSeries {
  public static final String MODEL_TAG_KEY = "_anomalyModel";
  
  protected final TimeSeries source;
  protected final TimeSeriesId id;
  
  public EgadsTimeSeries(final TimeSeries source, 
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
    tags.put(MODEL_TAG_KEY, model);
    builder.setTags(tags);
    this.id = builder.build();
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    return source.iterator(type);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    return source.iterators();
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return source.types();
  }

  @Override
  public void close() {
    // no-op
  }
  
}