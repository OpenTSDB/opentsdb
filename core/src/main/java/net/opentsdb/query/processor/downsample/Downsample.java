// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.rollup.RollupConfig;

/**
 * A processing node that performs downsampling on each individual time series
 * passed in as a result.
 * 
 * @since 3.0
 */
public class Downsample extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(Downsample.class);
  
  /** The non-null config. */
  private final DownsampleConfig config;
  
  /**
   * Default ctor.
   * @param factory A non-null {@link DownsampleFactory}.
   * @param context A non-null context.
   * @param id An ID for the node.
   * @param config A non-null {@link DownsampleConfig}.
   */
  public Downsample(final QueryNodeFactory factory, 
                    final QueryPipelineContext context,
                    final String id,
                    final DownsampleConfig config) {
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
    final DownsampleResult results = new DownsampleResult(next);
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
   * A downsample result that's a member class of the main node so that we share
   * the references to the config and node.
   */
  class DownsampleResult implements QueryResult, TimeSpecification {
    /** Countdown latch for closing the result set based on the upstreams. */
    private final CountDownLatch latch;
    
    /** The non-null query results. */
    private final QueryResult results;
    
    /** The new downsampler time series applied to the result's time series. */
    private final List<TimeSeries> downsamplers;
    
    /** The downsampled resolution. */
    private final ChronoUnit resolution;
    
    /** The snapped starting timestamp of the downsample span. */
    private final TimeStamp start;
    
    /** The snapped final timestamp of the downsample span. */
    private final TimeStamp end;
    
    /**
     * Default ctor that dumps each time series into a new Downsampler time series.
     * @param results The non-null results set.
     */
    DownsampleResult(final QueryResult results) {
      latch = new CountDownLatch(Downsample.this.upstream.size());
      this.results = results;
      downsamplers = Lists.newArrayListWithCapacity(results.timeSeries().size());
      for (final TimeSeries series : results.timeSeries()) {
        downsamplers.add(new DownsampleTimeSeries(series));
      }
      if (config.units() == null) {
        resolution = ChronoUnit.FOREVER;
      } else {
        switch (config.units()) {
        case NANOS:
        case MICROS:
          resolution = ChronoUnit.NANOS;
          break;
        case MILLIS:
          resolution = ChronoUnit.MILLIS;
          break;
        default:
          resolution = ChronoUnit.SECONDS;
        }
      }
      
      final SemanticQuery query = (SemanticQuery) context.query();
      if (config.runAll()) {
        start = query.startTime();
        end = query.endTime();
      } else if (config.timezone() != Const.UTC) {
        start = new ZonedNanoTimeStamp(query.startTime().epoch(), 
            query.startTime().nanos(), config.timezone());
        start.snapToPreviousInterval(config.intervalPart(), config.units());
        if (start.compare(Op.LT, query.startTime())) {
          nextTimestamp(start);
        }
        end = new ZonedNanoTimeStamp(query.endTime().epoch(), 
            query.endTime().nanos(), config.timezone());
        end.snapToPreviousInterval(config.intervalPart(), config.units());
        if (end.compare(Op.LTE, start)) {
          throw new IllegalArgumentException("Snapped end time: " + end 
              + " must be greater than the start time: " + start);
        }
      } else {
        start = query.startTime().getCopy();
        start.snapToPreviousInterval(config.intervalPart(), config.units());
        if (start.compare(Op.LT, query.startTime())) {
          nextTimestamp(start);
        }
        end = query.endTime().getCopy();
        end.snapToPreviousInterval(config.intervalPart(), config.units());
        if (end.compare(Op.LTE, start)) {
          throw new IllegalArgumentException("Snapped end time: " + end 
              + " must be greater than the start time: " + start);
        }
      }
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return this;
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
      return Downsample.this;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return results.idType();
    }
    
    @Override
    public ChronoUnit resolution() {
      return resolution;
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
    
    public void updateTimestamp(final int offset, final TimeStamp timestamp) {
      if (offset < 0) {
        throw new IllegalArgumentException("Negative offsets are not allowed.");
      }
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null.");
      }
      if (config.runAll()) {
        timestamp.update(start);
      } else {
        final TimeStamp increment = new ZonedNanoTimeStamp(
            start.epoch(), start.msEpoch(), start.timezone());
        for (int i = 0; i < offset; i++) {
          increment.add(config.duration());
        }
        timestamp.update(increment);
      }
    }
    
    public void nextTimestamp(final TimeStamp timestamp) {
      if (config.runAll()) {
        timestamp.update(start);
      } else {
        timestamp.add(config.duration());
      }
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }
    
    @Override
    public TimeStamp end() {
      return end;
    }
    
    @Override
    public TemporalAmount interval() {
      return config.interval();
    }

    @Override
    public ChronoUnit units() {
      return config.units();
    }

    @Override
    public ZoneId timezone() {
      return config.timezone();
    }
    
    /**
     * The super simple wrapper around the time series source that generates 
     * iterators using the factory.
     */
    class DownsampleTimeSeries implements TimeSeries {
      /** The non-null source. */
      private final TimeSeries source;
      
      /**
       * Default ctor.
       * @param source The non-null source to pull data from.
       */
      private DownsampleTimeSeries(final TimeSeries source) {
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
            ((ProcessorFactory) Downsample.this.factory()).newIterator(
                type, 
                Downsample.this, 
                DownsampleResult.this,
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
          iterators.add(((ProcessorFactory) Downsample.this.factory()).newIterator(
              type, 
              Downsample.this, 
              DownsampleResult.this,
              Lists.newArrayList(source)));
        }
        return iterators;
      }

      @Override
      public Collection<TypeToken<?>> types() {
        // TODO - join with the factories supported.
        return source.types();
      }

      @Override
      public void close() {
        source.close();
      }
      
    }

    
  }
  
}
