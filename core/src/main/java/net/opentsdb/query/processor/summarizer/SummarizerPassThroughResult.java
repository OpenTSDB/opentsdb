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
package net.opentsdb.query.processor.summarizer;

import java.util.Collection;
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
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.query.processor.summarizer.SummarizerNonPassThroughResult.SummarizerTimeSeries;

public class SummarizerPassThroughResult extends BaseWrappedQueryResult {

  /** The non-null parent node. */
  final Summarizer node;
  
  private final QueryResult results;
  
  /** The non-null list of summarizer time series. */
  private final List<TimeSeries> series;
  
  final Map<Long, TimeSeries> summarized_series;
  
  SummarizerPassThroughResult(final Summarizer node, final QueryResult results) {
    super(results);
    this.node = node;
    this.results = results;
    series = Lists.newArrayList();
    summarized_series = Maps.newConcurrentMap();
    for (final TimeSeries ts : results.timeSeries()) {
      series.add(new SummarizerPassThroughTimeSeries(ts));
    }
  }
  
  @Override
  public Collection<TimeSeries> timeSeries() {
    return series;
  }
  
  @Override
  public QueryNode source() {
    return results.source();
  }
  
  @Override
  public void close() {
    results.close();
    node.onNext(new SummarizerSummarizedResult());
  }
  
  Summarizer summarizerNode() {
    return node;
  }
  
  /**
   * Summarizer time series. 
   */
  class SummarizerPassThroughTimeSeries implements TimeSeries {
    /** The non-null source. */
    private final TimeSeries source;
    
    /**
     * Default ctor.
     * @param source The non-null source to pull data from.
     */
    private SummarizerPassThroughTimeSeries(final TimeSeries source) {
      this.source = source;
    }
    
    @Override
    public TimeSeriesId id() {
      return source.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      if (type == null) {
        throw new IllegalArgumentException("Type cannot be null.");
      }
      if (!source.types().contains(type)) {
        return Optional.empty();
      }
      
      // if we already have a summary then we just return a new underlying
      // iterator otherwise we need a new pass-through.
      final SummarizedTimeSeries sts = new SummarizedTimeSeries(
          SummarizerPassThroughResult.this, source);
      if (summarized_series.putIfAbsent(source.id().buildHashCode(), sts) != null) {
        return source.iterator(type);
      } else {
        if (type == NumericType.TYPE) {
          final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
              new SummarizerPassThroughNumericIterator(sts);
          return Optional.of(it);
        } else if (type == NumericArrayType.TYPE) {
          final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
              new SummarizerPassThroughNumericArrayIterator(sts);
          return Optional.of(it);
        } else if (type == NumericSummaryType.TYPE) {
          final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
              new SummarizerPassThroughNumericSummaryIterator(sts);
          return Optional.of(it);
        }
        return Optional.empty();
      }
    }
    
    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final Collection<TypeToken<? extends TimeSeriesDataType>> types = source.types();
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
          Lists.newArrayListWithCapacity(types.size());
      for (final TypeToken<? extends TimeSeriesDataType> type : types) {
        if (!((SummarizerFactory) node.factory()).types().contains(type)) {
          continue;
        }
        final SummarizedTimeSeries sts = new SummarizedTimeSeries(
            SummarizerPassThroughResult.this, source);
        if (summarized_series.putIfAbsent(source.id().buildHashCode(), sts) != null) {
          iterators.add(source.iterator(type).get());
        } else {
          if (type == NumericType.TYPE) {
            iterators.add(new SummarizerPassThroughNumericIterator(sts));
          } else if (type == NumericArrayType.TYPE) {
            iterators.add(new SummarizerPassThroughNumericArrayIterator(sts));
          } else if (type == NumericSummaryType.TYPE) {
            iterators.add(new SummarizerPassThroughNumericSummaryIterator(sts));
          }
        }
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      //source.close(); // waiting for the summarized series to close
    }
  }
  
  public class SummarizerSummarizedResult extends BaseWrappedQueryResult {
    
    SummarizerSummarizedResult() {
      super(results);
    }

    @Override
    public QueryNode source() {
      return node;
    }
    
    @Override
    public Collection<TimeSeries> timeSeries() {
      return summarized_series.values();
    }
    
    @Override
    public void close() {
      // no-op
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // always null
      return null;
    }
  }
}
