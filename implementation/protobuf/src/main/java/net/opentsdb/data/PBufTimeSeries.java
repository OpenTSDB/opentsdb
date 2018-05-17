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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.pbuf.TimeSeriesDataPB;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.serdes.PBufIteratorSerdes;
import net.opentsdb.query.serdes.PBufIteratorSerdesFactory;

/**
 * Wrapper around a protobuf {@link TimeSeries} implementation.
 * 
 * @since 3.0
 */
public class PBufTimeSeries implements TimeSeries {
  
  /** The serdes factory link. */
  private final PBufIteratorSerdesFactory factory;
  
  /** The protobuf time series. */
  private final TimeSeriesPB.TimeSeries time_series;
  
  /** The wrapper around the protobuf time series ID. */
  private PBufTimeSeriesId id;
  
  /** A map of data types to parsed time series. */
  private Map<TypeToken<?>, TimeSeriesDataPB.TimeSeriesData> data;
  
  /**
   * Default ctor.
   * @param factory A non-null factory.
   * @param time_series A non-null source time series.
   */
  public PBufTimeSeries(final PBufIteratorSerdesFactory factory, 
                        final TimeSeriesPB.TimeSeries time_series) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (time_series == null) {
      throw new IllegalArgumentException("Time series cannot be null.");
    }
    this.factory = factory;
    this.time_series = time_series;
    data = Maps.newHashMapWithExpectedSize(time_series.getDataCount());
    for (final TimeSeriesDataPB.TimeSeriesData data : 
          time_series.getDataList()) {
      try {
        final Class<?> clazz = Class.forName(data.getType());
        final TypeToken<?> type = TypeToken.of(clazz);
        this.data.put(type, data);
      } catch (ClassNotFoundException e) {
        throw new SerdesException("Failed to find a class for type: " 
            + data.getType(), e);
      }
    }
  }
  
  @Override
  public TimeSeriesId id() {
    if (id == null) {
      id = new PBufTimeSeriesId(time_series.getId());
    }
    return id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    TimeSeriesDataPB.TimeSeriesData series = data.get(type);
    if (series == null) {
      return Optional.empty();
    }
    PBufIteratorSerdes serdes = factory.serdesForType(type);
    if (serdes == null) {
      throw new SerdesException("Had data but unable to find a "
          + "deserializer for the type: " + type);
    }
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        serdes.deserialize(series);
    return Optional.of(iterator);
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithCapacity(data.size());
    for (final Entry<TypeToken<?>, TimeSeriesDataPB.TimeSeriesData> entry : 
          data.entrySet()) {
      PBufIteratorSerdes serdes = factory.serdesForType(entry.getKey());
      if (serdes == null) {
        throw new SerdesException("Had data but unable to find a "
            + "deserializer for the type: " + entry.getKey());
      }
      iterators.add(serdes.deserialize(entry.getValue()));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return data.keySet();
  }

  @Override
  public void close() {
    // no-op
  }

}
