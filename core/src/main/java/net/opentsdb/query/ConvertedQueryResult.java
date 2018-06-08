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
package net.opentsdb.query;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * A generic class to convert a result with {@link Const#TS_BYTE_ID}
 * encoded time series IDs to {@link Const#TS_STRING_ID} IDs. Call either
 * {@link #convert(QueryResult, QueryNode, Span)} or 
 * {@link #convert(QueryResult, QuerySink, Span)} and the given node or 
 * sink will have it's {@link QueryNode#onNext(QueryResult)} method 
 * called with the decoded result.
 * 
 * @since 3.0
 */
public class ConvertedQueryResult implements QueryResult, Runnable {
  /** The original encoded result. */
  private final QueryResult result;
  
  /** The node to callback with the converted result. If this is null
   * then the {@link #sink} cannot be null. */
  private final QueryNode node;
  
  /** The sink to callback with the converted result. If this is null
   * then the {@link #node} cannot be null. */
  private final QuerySink sink;
  
  /** An optional tracer span. */
  private final Span span;
  
  /** The list of converted time series. */
  private List<TimeSeries> series;
  
  /**
   * Protected ctor for nodes.
   * @param result The non-null query result.
   * @param node The non-null node.
   * @param span An optional tracer span.
   */
  protected ConvertedQueryResult(final QueryResult result, 
                                 final QueryNode node,
                                 final Span span) {
    this.result = result;
    this.node = node;
    sink = null;
    this.span = span;
  }
  
  /**
   * Protected ctor for sinks.
   * @param result The non-null query result.
   * @param sink The non-null sink.
   * @param span An optional tracer span.
   */
  protected ConvertedQueryResult(final QueryResult result, 
                                 final QuerySink sink,
                                 final Span span) {
    this.result = result;
    node = null;
    this.sink = sink;
    this.span = span;
  }
  
  /**
   * Performs the conversion. It's a public run method in case we want
   * to spool this off in another thread at some point. Just need to 
   * pass a thread pool in the {@code convert()} methods.
   */
  public void run() {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".run").start();
    } else {
      child = span;
    }
    
    // check to see if we actually need to do anything.
    if (result.idType() == Const.TS_STRING_ID) {
      series = null;
      if (child != null) {
        child.setSuccessTags()
             .setTag("note", "No conversion necessary.")
             .finish();
      }
      if (sink == null) {
        node.onNext(ConvertedQueryResult.this);
      } else {
        sink.onNext(ConvertedQueryResult.this);
      }
      return;
    }
    
    try {
      final Collection<TimeSeries> series_to_convert = result.timeSeries();
      series = Lists.newArrayListWithCapacity(series_to_convert.size());
      final List<Deferred<Object>> deferreds = 
          Lists.newArrayListWithCapacity(series_to_convert.size());
      for (final TimeSeries ts : series_to_convert) {
        deferreds.add(((TimeSeriesByteId) ts.id()).decode(false, child)
            .addCallback(new ConvertedTimeSeries(ts)));
      }
      
      class Complete implements Callback<Object, ArrayList<Object>> {
        @Override
        public Object call(ArrayList<Object> arg) throws Exception {
          if (child != null) {
            child.setSuccessTags()
                 .finish();
          }
          if (sink == null) {
            node.onNext(ConvertedQueryResult.this);
          } else {
            sink.onNext(ConvertedQueryResult.this);
          }
          return null;
        }
      }
      
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          if (child != null) {
            child.setErrorTags(ex)
                 .finish();
          }
          if (sink == null) {
            node.onError(ex);
          } else {
            sink.onError(ex);
          }
          return null;
        }
      }
      
      Deferred.group(deferreds)
        .addCallbacks(new Complete(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags(e)
             .finish();
      }
      if (sink == null) {
        node.onError(e);
      } else {
        sink.onError(e);
      }
    }
  }

  @Override
  public TimeSpecification timeSpecification() {
    return result.timeSpecification();
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return series == null ? result.timeSeries() : series;
  }

  @Override
  public long sequenceId() {
    return result.sequenceId();
  }

  @Override
  public QueryNode source() {
    return result.source();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return result.resolution();
  }
  
  @Override
  public RollupConfig rollupConfig() {
    return result.rollupConfig();
  }
  
  @Override
  public void close() {
    result.close();
  }
  
  /**
   * Converts the byte encoded result IDs to string IDs. Converted 
   * results are passed back to the node's 
   * {@link QueryNode#onNext(QueryResult)} method.
   * @param result The non-null result to convert.
   * @param node The non-null node to callback with the results.
   * @param span An optional tracing span.
   * @throws IllegalArgumentException if the node or result were null.
   */
  public static void convert(final QueryResult result, 
                             final QueryNode node, 
                             final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final ConvertedQueryResult converted = 
        new ConvertedQueryResult(result, node, span);
    converted.run();
  }
  
  /**
   * Converts the byte encoded result IDs to string IDs. Converted 
   * results are passed back to the node's 
   * {@link QuerySink#onNext(QueryResult)} method.
   * @param result The non-null result to convert.
   * @param sink The non-null sink to callback with the results.
   * @param span An optional tracing span.
   * @throws IllegalArgumentException if the sink or result were null.
   */
  public static void convert(final QueryResult result, 
                             final QuerySink sink, 
                             final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (sink == null) {
      throw new IllegalArgumentException("Sink cannot be null.");
    }
    final ConvertedQueryResult converted = 
        new ConvertedQueryResult(result, sink, span);
    converted.run();
  }
  
  /**
   * Class to hold the converted ID. All the other methods just pass through
   * to the time series source.
   */
  class ConvertedTimeSeries implements Callback<Object, TimeSeriesStringId>, 
      TimeSeries {
    /** The source time series. */
    private final TimeSeries source;
    
    /** The converted ID. */
    private TimeSeriesStringId id;
    
    /**
     * Package private ctor.
     * @param source The non-null source.
     */
    ConvertedTimeSeries(final TimeSeries source) {
      this.source = source;
    }
     
    @Override
    public TimeSeriesId id() {
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      return source.iterator(type);
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      return source.iterators();
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }

    @Override
    public Object call(final TimeSeriesStringId id) throws Exception { 
      this.id = id;
      synchronized (series) {
        series.add(this);
      }
      return null;
    }
    
  }
}
