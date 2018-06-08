// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.rate;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.rollup.RollupConfig;

/**
 * A processing node that performs rate conversion on each individual time series
 * passed in as a result.
 * 
 * @since 3.0
 */
public class Rate extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(Rate.class);
  
  /** The non-null config. */
  private final RateOptions config;
  
  public Rate(final QueryNodeFactory factory, 
              final QueryPipelineContext context, 
              final String id,
              final RateOptions config) {
    super(factory, context, id);
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    this.config = config;
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  @Override
  public void close() {
    // No-op
  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    for (final QueryNode us : upstream) {
      try {
        us.onComplete(this, final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onComplete on Node: " + us, e);
      }
    }
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final RateResult results = new RateResult(next);
    for (final QueryNode us : upstream) {
      try {
        us.onNext(results);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
        results.close();
      }
    }
  }

  @Override
  public void onError(final Throwable t) {
    for (final QueryNode us : upstream) {
      try {
        us.onError(t);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onError on Node: " + us, e);
      }
    }
  }
  
  /**
   * A rate result that's a member class of the main node so that we share
   * the references to the config and node.
   */
  class RateResult implements QueryResult {
    /** Countdown latch for closing the result set based on the upstreams. */
    private final CountDownLatch latch;
    
    /** The non-null query results. */
    private final QueryResult results;
    
    /** The new downsampler time series applied to the result's time series. */
    private final List<TimeSeries> downsamplers;
    
    /**
     * Default ctor that dumps each time series into a new Rate time series.
     * @param results The non-null results set.
     */
    private RateResult(final QueryResult results) {
      latch = new CountDownLatch(Rate.this.upstream.size());
      this.results = results;
      downsamplers = Lists.newArrayListWithCapacity(results.timeSeries().size());
      for (final TimeSeries series : results.timeSeries()) {
        downsamplers.add(new RateTimeSeries(series));
      }
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return results.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return downsamplers;
    }

    @Override
    public long sequenceId() {
      return results.sequenceId();
    }

    @Override
    public QueryNode source() {
      return Rate.this;
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
      // NOTE - a race here. Should be idempotent.
      latch.countDown();
      if (latch.getCount() <= 0) {
        results.close();
      }
    }

    
  }
  
  /**
   * The super simple wrapper around the time series source that generates 
   * iterators using the factory.
   */
  class RateTimeSeries implements TimeSeries {
    /** The non-null source. */
    private final TimeSeries source;
    
    /**
     * Default ctor.
     * @param source The non-null source to pull data from.
     */
    private RateTimeSeries(final TimeSeries source) {
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
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
          ((ProcessorFactory) Rate.this.factory()).newIterator(
              type, 
              Rate.this, 
              null,
              Lists.newArrayList(source));
      if (iterator != null) {
        return Optional.of(iterator);
      }
      return Optional.empty();
    }
    
    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      final Collection<TypeToken<?>> types = source.types();
      final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
          Lists.newArrayListWithCapacity(types.size());
      for (final TypeToken<?> type : types) {
        iterators.add(((ProcessorFactory) Rate.this.factory()).newIterator(
            type, 
            Rate.this, 
            null,
            Lists.newArrayList(source)));
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }

    @Override
    public void close() {
      source.close();
    }
    
  }
}
