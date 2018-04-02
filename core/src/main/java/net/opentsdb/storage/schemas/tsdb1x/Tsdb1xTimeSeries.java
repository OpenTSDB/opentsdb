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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.exceptions.IllegalDataException;

public class Tsdb1xTimeSeries implements TimeSeries {
  /** The time series ID for this set of data. */
  protected TSUID tsuid;
  
  /** A map of types to spans. */
  protected volatile Map<TypeToken<?>, Span<?>> data;
  
  /**
   * Default ctor.
   * @param tsuid A non-null and non-empty byte array conforming to the
   * schema.
   * @param schema A non-null schema (used to decode the tsuid)
   */
  public Tsdb1xTimeSeries(final byte[] tsuid, final Schema schema) {
    this.tsuid = new TSUID(tsuid, schema);
    // TODO - size properly according to data filters
    data = Maps.newHashMapWithExpectedSize(1);
  }
  
  @Override
  public TimeSeriesId id() {
    return tsuid;
  }

  @Override
  public Optional<Iterator<
      TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    final Span<?> span = data.get(type);
    if (span == null) {
      return Optional.empty();
    }
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    return Optional.of(it);
  }

  @Override
  public Collection<Iterator<
      TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<?>>> iterators = 
        Lists.newArrayListWithExpectedSize(data.size());
    for (final Span<?> sequences : data.values()) {
      iterators.add(sequences.iterator());
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return data.keySet();
  }

  @Override
  public void close() {
    // no-op.
  }
  
  /**
   * Adds the sequence to the span map using the type of the sequence.
   * Fetches a new span from the schema if necessary. Since it's in the
   * fast path we allow NPEs to bubble up if the sequence or schema
   * were null.
   * @param sequence A non-null sequence.
   * @param reversed Whether or not the sequence should be reversed.
   * @param keep_earliest Whether or not to keep the earliest data points
   * when deduping.
   * @param schema A non-null schema to generate new span from.
   * @throws NullPointerException if the sequence or span was null.
   */
  public void addSequence(final RowSeq sequence, 
                          final boolean reversed,
                          final boolean keep_earliest,
                          final Schema schema) {
    Span<?> span = data.get(sequence.type());
    if (span == null) {
      synchronized (this) {
        span = data.get(sequence.type());
        if (span == null) {
          span = schema.newSpan(sequence.type(), reversed);
          if (span == null) {
            throw new IllegalDataException("No span found for type: " 
                + sequence.type());
          }
          data.put(sequence.type(), span);
        }
      }
    }
    span.addSequence(sequence, keep_earliest);
  }
}
