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
package net.opentsdb.query.processor.summarizer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.ProcessorFactory;

/**
 * A result for summarizer nodes.
 * 
 * @since 3.0
 */
public class SummarizerNonPassThroughResult extends BaseWrappedQueryResult {

  /** The non-null parent node. */
  private final Summarizer node;
  
  /** The non-null list of summarizer time series. */
  private final List<TimeSeries> series;
  
  /**
   * Default ctor.
   * @param node The non-null node.
   * @param results The non-null results to source from.
   */
  SummarizerNonPassThroughResult(final Summarizer node, final QueryResult results) {
    super(results);
    this.node = node;
    series = Lists.newArrayList();
    for (final TimeSeries ts : results.timeSeries()) {
      series.add(new SummarizerTimeSeries(ts));
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    // NOTE This is always null as we don't want to generate arrays.
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return series;
  }
  
  @Override
  public QueryNode source() {
    return node;
  }
  
  /**
   * Summarizer time series. 
   */
  class SummarizerTimeSeries implements TimeSeries {
    /** The non-null source. */
    private final TimeSeries source;
    
    /**
     * Default ctor.
     * @param source The non-null source to pull data from.
     */
    private SummarizerTimeSeries(final TimeSeries source) {
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
      final TypedTimeSeriesIterator iterator = 
          ((ProcessorFactory) node.factory()).newTypedIterator(
              type, 
              node, 
              SummarizerNonPassThroughResult.this,
              Lists.newArrayList(source));
      if (iterator != null) {
        return Optional.of(iterator);
      }
      return Optional.empty();
    }
    
    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final Collection<TypeToken<? extends TimeSeriesDataType>> types = source.types();
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
          Lists.newArrayListWithCapacity(types.size());
      for (final TypeToken<?> type : types) {
        if (!((SummarizerFactory) node.factory()).types().contains(type)) {
          continue;
        }
        iterators.add(((ProcessorFactory) node.factory()).newTypedIterator(
            NumericSummaryType.TYPE, 
            node, 
            SummarizerNonPassThroughResult.this,
            Lists.newArrayList(source)));
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      // TODO - join with the factories supported.
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }

    @Override
    public void close() {
      source.close();
    }
  }
}
