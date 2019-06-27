// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.stats;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;

import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.TimeSeriesDataSourceConfig;

/**
 * A simple default implementation of the Query Stats object. It simply takes a
 * trace for now. It will start a new span if the trace is not null.
 * 
 * TODO - flesh this out as we flesh out the stats interface.
 * 
 * @since 3.0
 */
public class DefaultQueryStats implements QueryStats {
  private static final String RAW_DATA_METRIC = "storage.data.bytes";
  private static final String RAW_TIMESERIES_METRIC = "storage.timeseries.count";
  private static final String SERIALIZED_DATA_METRIC = "serialized.data.bytes";
  private static final String SERIALIZED_TIMESERIES_METRIC = "serialized.timeseries.count";
  
  private QueryContext context;
  private final Trace trace;
  private final Span query_span;
  private final AtomicLong raw_data_size;
  private final AtomicLong raw_time_series_count;
  private final AtomicLong serialized_data_size;
  private final AtomicLong serialized_time_series_count;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   */
  DefaultQueryStats(final Builder builder) {
    trace = builder.trace;
    query_span = builder.query_span;
    raw_data_size = new AtomicLong();
    raw_time_series_count = new AtomicLong();
    serialized_data_size = new AtomicLong();
    serialized_time_series_count = new AtomicLong();
  }
  
  @Override
  public void setQueryContext(final QueryContext context) {
    this.context = context;
  }
  
  @Override
  public Trace trace() {
    return trace;
  }
  
  @Override
  public Span querySpan() {
    return query_span;
  }
  
  @Override
  public void emitStats() {
    if (context == null) {
      throw new IllegalStateException("Context wasn't set!");
    }
    
    final StatsCollector stats = context.tsdb().getStatsCollector();
    if (stats == null) {
      return;
    }
 
    // extract a namespace if possible.
    String namespace = null;
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      if (config instanceof TimeSeriesDataSourceConfig) {
        String local_namespace = ((TimeSeriesDataSourceConfig) config).getNamespace();
        if (Strings.isNullOrEmpty(local_namespace)) {
          // extract from metric filter.
          local_namespace = ((TimeSeriesDataSourceConfig) config).getMetric().getMetric();
          local_namespace = local_namespace.substring(0, local_namespace.indexOf('.'));
        }
        
        if (namespace == null) {
          namespace = local_namespace;
        } else {
          if (!namespace.equals(local_namespace)) {
            // Different namespaces so we won't use one. 
            namespace = "MultipleNameSpaces";
            break;
          }
        }
      }
    }
    
    final String[] tags = new String[] { 
        context.authState() != null ? 
            context.authState().getUser() : "Unkown",
        "namespace", namespace };
    
    stats.setGauge(RAW_DATA_METRIC, raw_data_size.get(), tags);
    stats.setGauge(RAW_TIMESERIES_METRIC, raw_time_series_count.get(), tags);
    stats.setGauge(SERIALIZED_DATA_METRIC, serialized_data_size.get(), tags);
    stats.setGauge(SERIALIZED_TIMESERIES_METRIC, serialized_time_series_count.get(), tags);
  }
  
  public void incrementRawDataSize(final long size) {
    raw_data_size.addAndGet(size);
  }
  
  public void incrementSerializedDataSize(final long size) {
    serialized_data_size.addAndGet(size);
  }
  
  public void incrementRawTimeSeriesCount(final long count) {
    raw_time_series_count.addAndGet(count);
  }
  
  public void incrementSerializedTimeSeriesCount(final long count) {
    serialized_time_series_count.addAndGet(count);
  }
  
  public long rawDataSize() {
    return raw_data_size.get();
  }
  
  public long serializedDataSize() {
    return serialized_data_size.get();
  }
  
  public long rawTimeSeriesCount() {
    return raw_time_series_count.get();
  }
  
  public long serializedTimeSeriesCount() {
    return serialized_time_series_count.get();
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Builder for the {@link DefaultQueryStats} class.
   */
  public static class Builder {
    private Trace trace;
    private Span query_span;
    
    /**
     * @param trace An optional trace.
     * @return The builder.
     */
    public Builder setTrace(final Trace trace) {
      this.trace = trace;
      return this;
    }
    
    public Builder setQuerySpan(final Span query_span) {
      this.query_span = query_span;
      return this;
    }
    
    /** @return An instantiated {@link DefaultQueryStats} object. */
    public QueryStats build() {
      return new DefaultQueryStats(this);
    }
  }
  
}
