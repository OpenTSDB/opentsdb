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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedIterator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.rollup.RollupConfig;

/**
 * A result for summarizer nodes.
 * 
 * @since 3.0
 */
public class SummarizerResult implements QueryResult {

  /** The non-null parent node. */
  private final Summarizer node;
  
  /** The non-null query results. */
  private final QueryResult results;
  
  /** The non-null list of summarizer time series. */
  private final List<TimeSeries> series;
  
  /**
   * Default ctor.
   * @param node The non-null node.
   * @param results The non-null results to source from.
   */
  SummarizerResult(final Summarizer node, final QueryResult results) {
    this.node = node;
    this.results = results;
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
  public long sequenceId() {
    return results.sequenceId();
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public String dataSource() {
    return results.dataSource();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return results.idType();
  }

  @Override
  public ChronoUnit resolution() {
    return results.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return results.rollupConfig();
  }

  @Override
  public void close() {
    results.close();
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
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        final TypeToken<?> type) {
      if (type == null) {
        throw new IllegalArgumentException("Type cannot be null.");
      }
      if (!source.types().contains(type)) {
        return Optional.empty();
      }
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
          ((ProcessorFactory) node.factory()).newTypedIterator(
              type, 
              node, 
              SummarizerResult.this,
              Lists.newArrayList(source));
      if (iterator != null) {
        return Optional.of(iterator);
      }
      return Optional.empty();
    }
    
    @Override
    public Collection<TypedIterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      final Collection<TypeToken<?>> types = source.types();
      final List<TypedIterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators =
          Lists.newArrayListWithCapacity(types.size());
      for (final TypeToken<?> type : types) {
        if (!((SummarizerFactory) node.factory()).types().contains(type)) {
          continue;
        }
        iterators.add(((ProcessorFactory) node.factory()).newTypedIterator(
            NumericSummaryType.TYPE, 
            node, 
            SummarizerResult.this,
            Lists.newArrayList(source)));
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      // TODO - join with the factories supported.
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }

    @Override
    public void close() {
      source.close();
    }
  }
}
