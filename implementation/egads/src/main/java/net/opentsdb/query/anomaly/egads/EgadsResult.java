// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;

/**
 * A result from EGADs. The class is meant to take the "current" results and
 * a cached prediction (for a wider timespan). It will then align the prediction
 * time series (which must be NumericArrayType.TYPEs) with the current window
 * given a time specification for the current window. 
 * 
 * TODO - what if the current window is NOT a downsampled result with a timespec?
 * 
 * TODO - triple check the alignment logic.
 * 
 * @since 3.0
 */
public class EgadsResult implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(EgadsResult.class);
  
  protected final QueryNode node;
  protected final QueryResult original_result;
  protected final List<TimeSeries> series;

  public EgadsResult(final QueryNode node, 
                     final QueryResult original_result, 
                     final boolean include_observed) {
    this.node = node;
    this.original_result = original_result;
    if (include_observed) {
      series = Lists.newArrayList(original_result.timeSeries());
    } else {
      series = Lists.newArrayList();
    }
  }

  public void addPredictionsAndThresholds(final TimeSeries ts, 
                                          final QueryResult[] results) {
    if (ts.types().contains(NumericArrayType.TYPE)) {
      series.add(new AlignedArrayTimeSeries(ts, results));
    } else {
      series.add(ts);
    }
  }

  @Override
  public TimeSpecification timeSpecification() {
    return original_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return original_result.dataSource();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return original_result.idType();
  }

  @Override
  public ChronoUnit resolution() {
    return original_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return original_result.rollupConfig();
  }

  @Override
  public void close() {
    original_result.close();
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  class AlignedArrayTimeSeries implements TimeSeries {
    final TimeSeries source;
    final QueryResult[] results;

    AlignedArrayTimeSeries(final TimeSeries source, final QueryResult[] results) {
      this.source = source;
      this.results = results;
    }

    @Override
    public TimeSeriesId id() {
      return source.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (type != NumericArrayType.TYPE) {
        return source.iterator(type);
      }
      return Optional.of(new ArrayIterator());
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its =
          Lists.newArrayListWithExpectedSize(1);
      for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
        if (type == NumericArrayType.TYPE) {          
          its.add(new ArrayIterator());
        } else {
          its.add(source.iterator(type).get());
        }
      }
      return its;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }

    class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
      TimeSeriesValue<NumericArrayType>, NumericArrayType {

      final TimeSeriesValue<NumericArrayType> value;
      final int start_idx;
      final int end_idx;
      boolean has_next;

      ArrayIterator() {
        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            source.iterator(NumericArrayType.TYPE);
        if (!op.isPresent()) {
          value = null;
          start_idx = end_idx = -1;
          return;
        }

        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
            op.get();
        if (!iterator.hasNext()) {
          value = null;
          start_idx = end_idx = -1;
          return;
        }

        value = (TimeSeriesValue<NumericArrayType>) iterator.next();
        TimeStamp ts = results[0].timeSpecification().start().getCopy();
        int i = value.value().offset();
        int x = i;
        while (ts.compare(Op.LT, original_result.timeSpecification().start())) {
          i++;
          x++;
          ts.add(original_result.timeSpecification().interval());
        }

        while (ts.compare(Op.LT, original_result.timeSpecification().end())) {
          x++;
          ts.add(original_result.timeSpecification().interval());
        }

        if (value.value().isInteger()&& x > value.value().longArray().length) {
          LOG.error("Alignment error: x " + x + " was longer than the array: " 
              + value.value().longArray().length);
          x = value.value().longArray().length;
        } else if (x > value.value().doubleArray().length) {
          LOG.error("Alignment error x " + x + " was longer than the array: " 
              + value.value().doubleArray().length);
          x = value.value().doubleArray().length;
        }

        start_idx = i;
        end_idx = Math.min(x, value.value().end());
        has_next = end_idx > start_idx;
      }

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
        return value.timestamp();
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
        return start_idx;
      }

      @Override
      public int end() {
        return end_idx;
      }

      @Override
      public boolean isInteger() {
        return value.value().isInteger();
      }

      @Override
      public long[] longArray() {
        return value.value().longArray();
      }

      @Override
      public double[] doubleArray() {
        return value.value().doubleArray();
      }

    }
  }
  
}
