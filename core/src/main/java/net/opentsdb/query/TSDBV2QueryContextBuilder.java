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
package net.opentsdb.query;

import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

/**
 * The default implementation of a context builder.
 * 
 * TODO - more work coming on this.
 * 
 * @since 3.0
 */
public class TSDBV2QueryContextBuilder implements QueryContextBuilder {
  /** The TSDB to pull configs and settings from. */
  private TSDB tsdb;
  
  /** The list of sinks to callback with results. */
  private List<QuerySink> sinks;
  
  /** The query. */
  private TimeSeriesQuery query;
  
  /** The mode of operation. */
  private QueryMode mode;
  
  /** The stats object. */
  private QueryStats stats;
  
  /** Whether or not this object was built users can't call the set methods. */
  private boolean built;
  
  /**
   * Static helper to construct the builder.
   * @param tsdb A non-null TSDB object.
   * @return The builder.
   * @throws IllegalArgumentException if the TSDB object was null.
   */
  public static QueryContextBuilder newBuilder(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    final TSDBV2QueryContextBuilder builder = new TSDBV2QueryContextBuilder();
    builder.tsdb = tsdb;
    return builder;
  }
  
  @Override
  public QueryContextBuilder addQuerySink(final QuerySink listener) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    if (sinks == null) {
      sinks = Lists.newArrayListWithExpectedSize(1);
    }
    sinks.add(listener);
    return this;
  }

  @Override
  public QueryContextBuilder setQuerySinks(
      final Collection<QuerySink> listeners) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.sinks = Lists.newArrayList(listeners);
    return this;
  }

  @Override
  public QueryContextBuilder setQuery(final TimeSeriesQuery query) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.query = query;
    return this;
  }

  @Override
  public QueryContextBuilder setMode(final QueryMode mode) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.mode = mode;
    return this;
  }

  @Override
  public QueryContextBuilder setStats(final QueryStats stats) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.stats = stats;
    return this;
  }

  @Override
  public QueryContext build() {
    if (sinks == null || sinks.isEmpty()) {
      throw new IllegalArgumentException("At least one sink must be provided.");
    }
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (mode == null) {
      throw new IllegalArgumentException("Query mode cannot be null.");
    }
    built = true;
    return new LocalContext();
  }

  /**
   * Local implementation of a context. Just reads the variables from the
   * builder for now.
   */
  class LocalContext implements QueryContext {
    /** The downstream pipeline context. */
    private QueryPipelineContext context;
    
    /** A local span for tracing. */
    private Span local_span;
    
    public LocalContext() {
      if (stats != null && stats.trace() != null) {
        local_span = stats.trace().newSpan("Query Context Initialization")
            .asChildOf(stats.querySpan())
            .start();
      }
      context = new LocalPipeline(tsdb, query, this, buildGraph(), sinks);
      context.initialize(local_span);
    }
    
    @Override
    public Collection<QuerySink> sinks() {
      return sinks;
    }

    @Override
    public QueryMode mode() {
      return mode;
    }

    @Override
    public void fetchNext(final Span span) {
      context.fetchNext(span);
    }

    @Override
    public void close() {
      context.close();
      if (local_span != null) {
        // TODO - more stats around the context
        local_span.finish();
      }
    }

    @Override
    public QueryStats stats() {
      return stats;
    }
    
    @Override
    public TimeSeriesQuery query() {
      return query;
    }

    ExecutionGraph buildGraph() {
      final net.opentsdb.query.pojo.TimeSeriesQuery q = 
          (net.opentsdb.query.pojo.TimeSeriesQuery) query;
      List<ExecutionGraphNode> nodes = Lists.newArrayList();
      List<QueryNodeConfig> node_configs = Lists.newArrayList();
      
      for (final Metric metric : q.getMetrics()) {
        final net.opentsdb.query.pojo.TimeSeriesQuery.Builder sub_query = 
            net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
            .setTime(q.getTime())
            .addMetric(metric);
        if (!Strings.isNullOrEmpty(metric.getFilter())) {
          sub_query.addFilter(q.getFilter(metric.getFilter()));
        }
        
        final String interpolator;
        String agg = !Strings.isNullOrEmpty(metric.getAggregator()) ?
            metric.getAggregator().toLowerCase() : 
              q.getTime().getAggregator().toLowerCase();
        if (agg.contains("zimsum") || 
            agg.contains("mimmax") ||
            agg.contains("mimmin")) {
          interpolator = null;
        } else {
          interpolator = "LERP";
        }
        
        String previous_node = metric.getId();
        final QuerySourceConfig config = QuerySourceConfig.newBuilder()
            .setId(metric.getId())
            .setQuery(sub_query.build())
            .setConfiguration(tsdb.getConfig())
            .build();
        node_configs.add(config);
        
        nodes.add(ExecutionGraphNode.newBuilder()
            .setId(metric.getId())
            .setType("DataSource")
            .build());
        
        final Downsampler downsampler = metric.getDownsampler() != null ? 
            metric.getDownsampler() : q.getTime().getDownsampler();
         // downsample
        if (downsampler != null) {
          FillPolicy policy = downsampler.getFillPolicy().getPolicy();
          DownsampleConfig.Builder ds = (DownsampleConfig.Builder) DownsampleConfig.newBuilder()
              .setAggregator(downsampler.getAggregator())
              .setInterval(downsampler.getInterval())
              .setFill(downsampler.getFillPolicy() != null ? true : false)
              .setQuery(q)
              .setId("downsample_" + metric.getId())
              .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(policy)
                        .setRealFillPolicy(FillWithRealPolicy.NONE)
                        .setId(interpolator)
                        .setType(NumericType.TYPE.toString())
                        .build());
          if (!Strings.isNullOrEmpty(downsampler.getTimezone())) {
            ds.setTimeZone(ZoneId.of(downsampler.getTimezone()));
          }
          
          node_configs.add(ds.build());
          nodes.add(ExecutionGraphNode.newBuilder()
              .setId("downsample_" + metric.getId())
              .setType("Downsample")
              .addSource(previous_node)
              .build());
          previous_node = "downsample_" + metric.getId();
        }
        
        // Rate!
        if (metric.isRate()) {
          node_configs.add(RateOptions.newBuilder(metric.getRateOptions())
              .setId("rate_" + metric.getId())
              .build());
          nodes.add(ExecutionGraphNode.newBuilder()
            .setId("rate_" + metric.getId())
            .setType("Rate")
            .addSource(previous_node)
            .build());
          previous_node = "rate_" + metric.getId();
        }
        
        // Group by!
        final Filter filter = Strings.isNullOrEmpty(metric.getFilter()) ? null 
            : q.getFilter(metric.getFilter());
        if (filter != null) {
          GroupByConfig.Builder gb_config = null;
          final Set<String> join_keys = Sets.newHashSet();
          for (TagVFilter v : filter.getTags()) {
            if (v.isGroupBy()) {
              if (gb_config == null) {
                FillPolicy policy = downsampler == null ? 
                    FillPolicy.NONE : downsampler.getFillPolicy().getPolicy();
                gb_config = (GroupByConfig.Builder) GroupByConfig.newBuilder()
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(policy)
                        .setRealFillPolicy(FillWithRealPolicy.NONE)
                        .setId(interpolator)
                        .setType(NumericType.TYPE.toString())
                        .build())
                    .setId("groupBy_" + metric.getId());
              }
              join_keys.add(v.getTagk());
            }
          }

          if (gb_config != null) {
            gb_config.setTagKeys(join_keys);
            gb_config.setAggregator( 
                !Strings.isNullOrEmpty(metric.getAggregator()) ?
                metric.getAggregator() : q.getTime().getAggregator());
            
            node_configs.add(gb_config.build());
            nodes.add(ExecutionGraphNode.newBuilder()
                .setId("groupBy_" + metric.getId())
                .setType("GroupBy")
                .addSource(previous_node)
                .build());
            previous_node = "groupBy_" + metric.getId();
          }
        } else if (!agg.toLowerCase().equals("none")) {
          // we agg all 
          FillPolicy policy = downsampler == null ? 
              FillPolicy.NONE : downsampler.getFillPolicy().getPolicy();
          GroupByConfig.Builder gb_config = (GroupByConfig.Builder) GroupByConfig.newBuilder()
              .setGroupAll(true)
              .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                  .setFillPolicy(policy)
                  .setRealFillPolicy(FillWithRealPolicy.NONE)
                  .setId(interpolator)
                  .setType(NumericType.TYPE.toString())
                  .build())
              .setId("groupBy_" + metric.getId());
              
          gb_config.setAggregator(
              !Strings.isNullOrEmpty(metric.getAggregator()) ?
              metric.getAggregator() : q.getTime().getAggregator());
          
          node_configs.add(gb_config.build());
          nodes.add(ExecutionGraphNode.newBuilder()
              .setId("groupBy_" + metric.getId())
              .setType("GroupBy")
              .addSource(previous_node)
              .build());
          previous_node = "groupBy_" + metric.getId();
        }
      }
      
      final ExecutionGraph graph = ExecutionGraph.newBuilder()
          .setId("TsdbV2Query")
          .setNodes(nodes)
          .build();
      graph.setNodeConfigs(node_configs);
      
      return graph;
    }
  }
  
  class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(TSDB tsdb, TimeSeriesQuery query, QueryContext context,
        ExecutionGraph execution_graph, Collection<QuerySink> sinks) {
      super(tsdb, query, context, execution_graph, sinks);
    }

    @Override
    public void initialize(Span span) {
      final Span child;
      if (span != null) {
        child = span.newChild(getClass().getSimpleName() + ".initialize()")
                     .start();
      } else {
        child = null;
      }
      
      initializeGraph(child);
      if (child != null) {
        child.setSuccessTags().finish();
      }
    }

    @Override
    public String id() {
      return "TsdbV2Pipeline";
    }
    
  }
}
