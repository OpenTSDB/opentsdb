// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import net.opentsdb.data.TypedTimeSeriesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.processor.ProcessorFactory;

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
   * @param config A non-null {@link DownsampleConfig}.
   */
  public Downsample(final QueryNodeFactory factory, 
                    final QueryPipelineContext context,
                    final DownsampleConfig config) {
    super(factory, context);
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
  public void onNext(final QueryResult next) {
    final DownsampleResult results = new DownsampleResult(next);
    Collection<QueryNode> upstream = this.upstream;
    for (final QueryNode us : upstream) {
      try {
        us.onNext(results);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
        results.close();
      }
    }
  }
  
  /**
   * A downsample result that's a member class of the main node so that we share
   * the references to the config and node.
   */
  public class DownsampleResult extends BaseWrappedQueryResult 
      implements TimeSpecification {
    /** Countdown latch for closing the result set based on the upstreams. */
    private final CountDownLatch latch;
    
    /** The new downsampler time series applied to the result's time series. */
    private final List<TimeSeries> downsamplers;
    
    /** The downsampled resolution. */
    private final ChronoUnit resolution;
    
    /** The snapped starting timestamp of the downsample span. */
    private final TimeStamp start;
    
    /** The snapped final timestamp of the downsample span. */
    private final TimeStamp end;
    private boolean processInParallel;

    /**
     * Default ctor that dumps each time series into a new Downsampler time series.
     * @param results The non-null results set.
     */
    public DownsampleResult(final QueryResult results) {
      super(Downsample.this, results);
      latch = new CountDownLatch(Downsample.this.upstream.size());
      downsamplers = Lists.newArrayListWithCapacity(results.timeSeries().size());
      for (final TimeSeries series : results.timeSeries()) {
        downsamplers.add(new DownsampleTimeSeries(series));
        if (!this.processInParallel && config.getProcessAsArrays()
            && (series.types().contains(NumericArrayType.TYPE)
                || series.types().contains(NumericType.TYPE))) {
          this.processInParallel = true;
        }
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
      if (config.getRunAll()) {
        start = query.startTime();
        end = query.endTime();
      } else if (!config.timezone().equals(Const.UTC)) {
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
      return config.getFill() ? this : result.timeSpecification();
    }

    @Override
    public List<TimeSeries> timeSeries() {
      return downsamplers;
    }

    @Override
    public ChronoUnit resolution() {
      return resolution;
    }
    
    public void updateTimestamp(final int offset, final TimeStamp timestamp) {
      if (offset < 0) {
        throw new IllegalArgumentException("Negative offsets are not allowed.");
      }
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null.");
      }
      if (config.getRunAll()) {
        timestamp.update(start);
      } else {
        final TimeStamp increment = new ZonedNanoTimeStamp(
            start.epoch(), start.msEpoch(), start.timezone());
        for (int i = 0; i < offset; i++) {
          increment.add(config.interval());
        }
        timestamp.update(increment);
      }
    }
    
    public void nextTimestamp(final TimeStamp timestamp) {
      if (config.getRunAll()) {
        timestamp.update(start);
      } else {
        timestamp.add(config.interval());
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
    public String stringInterval() {
      return config.getInterval();
    }
    
    @Override
    public ChronoUnit units() {
      return config.units();
    }

    @Override
    public ZoneId timezone() {
      return config.timezone();
    }
    
    /** @return The downstream results. */
    QueryResult downstreamResult() {
      return result;
    }
    
    /**
     * The super simple wrapper around the time series source that generates 
     * iterators using the factory.
     */
    public class DownsampleTimeSeries implements TimeSeries {
      /** The non-null source. */
      private final TimeSeries source;
      
      /**
       * Default ctor.
       * @param source The non-null source to pull data from.
       */
      public DownsampleTimeSeries(final TimeSeries source) {
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
        
        final TypedTimeSeriesIterator iterator = 
            ((ProcessorFactory) Downsample.this.factory()).newTypedIterator(
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
      public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
        final Collection<TypeToken<? extends TimeSeriesDataType>> types = source.types();
        final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
            Lists.newArrayListWithCapacity(types.size());
        
        for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
          iterators.add(((ProcessorFactory) Downsample.this.factory()).newTypedIterator(
              type, 
              Downsample.this, 
              DownsampleResult.this,
              Lists.newArrayList(source)));
        }
        return iterators;
      }

      @Override
      public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
        // TODO - join with the factories supported.
        if (config.getFill() && 
            config.getProcessAsArrays() && 
            (source.types().contains(NumericType.TYPE)
              || source.types().contains(NumericArrayType.TYPE))) {
          return Lists.newArrayList(NumericArrayType.TYPE);
        } else {
          return source.types();
        }
      }

      @Override
      public void close() {
        source.close();
      }
      
    }

    @Override
    public boolean processInParallel() {
      return processInParallel;
    }
  }
  
}
