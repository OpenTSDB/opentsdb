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
package net.opentsdb.query;

import java.io.IOException;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import net.opentsdb.meta.MetaQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.PreAggConfig.MetricPattern;
import net.opentsdb.query.PreAggConfig.TagsAndAggs;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.query.filter.NestedQueryFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

/**
 * Stub class for a super simple context filter that filters on the user and
 * headers for now.
 * 
 * TODO - Can cache the fetches from the config class for effeciency.
 *  
 * @since 3.0
 */
public class DefaultQueryContextFilter extends BaseTSDBPlugin 
    implements QueryContextFilter {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultQueryContextFilter.class);
  
  protected static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAP =
      new TypeReference<Map<String, Map<String, String>>>() { };
  protected static final TypeReference<
    Map<String, Map<String, Map<String, String>>>> HEADER_FILTERS =
      new TypeReference<Map<String, Map<String, Map<String, String>>>>() { };
  protected static final String HEADER_KEY = "tsd.queryfilter.filter.headers";
  protected static final String USER_KEY = "tsd.queryfilter.filter.users";
  protected static final String PREAGG_KEY = "tsd.queryfilter.filter.preagg";
  protected static final String METRIC_KEY = "tsd.queryfilter.filter.metric";
  protected static final String DEFAULT_CACHE_MODE = "tsd.queryfilter.cache.mode.default";
  
  protected static final String FILTER_CACHE_MODE = "cacheMode";
  protected static final String FILTER_BLACKLIST = "blacklist";
  protected static final String FILTER_ROLLUP = "rollupOverride";
  protected static final String FILTER_BYTE_LIMIT = "byteLimit";
  
  protected static final QueryExecutionException BLACKLISTED = 
      new QueryExecutionException("Access forbidden due to blacklist.", 403);
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    if (!tsdb.getConfig().hasProperty(HEADER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(HEADER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(HEADER_FILTERS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(USER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(USER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MAP_OF_MAP)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(PREAGG_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(PREAGG_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(PreAggConfig.TYPE_REF)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(METRIC_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(METRIC_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MetricFilters.class)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(DEFAULT_CACHE_MODE)) {
      tsdb.getConfig().register(DEFAULT_CACHE_MODE, null, true, 
          "The default cache mode, can be null.");
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSeriesQuery filter(final TimeSeriesQuery query, 
                                final AuthState auth_state, 
                                final Map<String, String> headers) {
    SemanticQuery.Builder builder = filterHeaders(query, headers);
    builder = filterUsers(builder, query, auth_state);
    builder = filterPreAggs(builder, query);
    builder = filterMetrics(builder, query);
    builder = filterCacheMode(builder, query);
    return builder != null ? builder.build() : query;
  }

  @Override
  public MetaQuery filter(final MetaQuery query,
                          final AuthState auth_state,
                          final Map<String, String> headers) {
    // NO-OP for now.
    return query;
  }

  @Override
  public String type() {
    return "DefaultQueryContextFilter";
  }
  
  SemanticQuery.Builder filterHeaders(final TimeSeriesQuery query, 
                                      final Map<String, String> headers) {
    SemanticQuery.Builder builder = null;
    Map<String, Map<String, Map<String, String>>> hdr_filter = 
        tsdb.getConfig().getTyped(HEADER_KEY, HEADER_FILTERS);
    if (hdr_filter == null || hdr_filter.isEmpty() || headers == null) {
      return builder;
    }
    
    for (final Entry<String, String> entry : headers.entrySet()) {
      Map<String, Map<String, String>> header_filter = 
          hdr_filter.get(entry.getKey());
      if (header_filter != null) {
        Map<String, String> values = header_filter.get(entry.getValue());
        if (values != null) {
          // OVERRIDE the query!
          if (builder == null) {
            builder = ((SemanticQuery) query).toBuilder();
          }
          
          String ov = values.get(FILTER_CACHE_MODE);
          if (ov != null && query.getCacheMode() == null) {
            builder.setCacheMode(CacheMode.valueOf(ov));
            LOG.trace("Overriding cache mode for header: " + 
                entry.getKey() + ":" + entry.getValue() + " to " + 
                CacheMode.valueOf(ov));
          }
          
          ov = values.get(FILTER_BLACKLIST);
          if (ov != null && Boolean.parseBoolean(ov)) {
            throw BLACKLISTED;
          }
        }
      }
    }
    
    return builder;
  }
  
  SemanticQuery.Builder filterUsers(final SemanticQuery.Builder upstream_builder,
                                    final TimeSeriesQuery query,
                                    final AuthState auth_state) {
    SemanticQuery.Builder builder = upstream_builder;
    Map<String, Map<String, String>> user_filters = 
        tsdb.getConfig().getTyped(USER_KEY, MAP_OF_MAP);
    if (user_filters == null) {
      return builder;
    }
    
    final String user = auth_state != null && 
        auth_state.getPrincipal() != null ? 
            auth_state.getPrincipal().getName() : "Unknown";
    Map<String, String> filter = user_filters.get(user);
    if (filter != null) {
      // OVERRIDE the query!
      if (builder == null) {
        builder = ((SemanticQuery) query).toBuilder();
      }
      String ov = filter.get(FILTER_CACHE_MODE);
      if (ov != null && query.getCacheMode() == null) {
        builder.setCacheMode(CacheMode.valueOf(ov));
        if (LOG.isTraceEnabled()) {
          LOG.trace("Overriding cache mode for user: " + user + " to " 
              + CacheMode.valueOf(ov));
        }
      }
      
      ov = filter.get(FILTER_BLACKLIST);
      if (ov != null && Boolean.parseBoolean(ov)) {
        throw BLACKLISTED;
      }
    }
    return builder;
  }

  SemanticQuery.Builder filterPreAggs(final SemanticQuery.Builder upstream_builder,
                                      final TimeSeriesQuery query) {
    // TODO - tons of stuff to do here, e.g. nulls from the group by or the 
    // filters.
    SemanticQuery.Builder builder = upstream_builder;
    Map<String, PreAggConfig> preagg_filters = 
        tsdb.getConfig().getTyped(PREAGG_KEY, PreAggConfig.TYPE_REF);
    if (preagg_filters == null || preagg_filters.isEmpty()) {
      return builder;
    }
    
    Map<String, QueryNodeConfig> rebuilt = Maps.newHashMap();
    boolean rebuild = false;
    MutableGraph<QueryNodeConfig> graph = null;
    
    for (final QueryNodeConfig config : query.getExecutionGraph()) {
      if (!(config instanceof TimeSeriesDataSourceConfig)) {
        continue;
      }
      
      TimeSeriesDataSourceConfig tsdc = (TimeSeriesDataSourceConfig) config;
      if (tsdc.getMetric() == null) {
        continue;
      }
      
      // TODO - namespace field
      final String namespace = tsdc.getMetric().getMetric().substring(0, 
          tsdc.getMetric().getMetric().indexOf('.'));
      final PreAggConfig preagg = preagg_filters.get(namespace);
      if (preagg == null) {
        continue;
      }
      
      // match the metric to a rule
      final String metric = tsdc.getMetric().getMetric().substring(
          tsdc.getMetric().getMetric().indexOf('.') + 1);
      MetricPattern metric_pattern = null;
      for (final MetricPattern pattern : preagg.getMetrics()) {
        if (pattern.getPattern().matcher(metric).find()) {
          metric_pattern = pattern;
          break;
        }
      }
      
      // No rules so we don't need to add "raw".
      if (metric_pattern == null) {
        continue;
      }
      
      // make sure we actually have an aggregation, otherwise it's pointless
      // to look for rules.
      if (graph == null) {
        graph = buildGraph(query);
      }
      final String agg = findAgg(tsdc.getId(), graph, tsdc);
      if (agg == null) {
        tsdc = rebuildRaw(tsdc, rebuilt);
        rebuild = true;
        continue;
      }
      
      // next we need to tag keys and see if we match
      Set<String> desired_tag_keys = null;
      if (!(Strings.isNullOrEmpty(tsdc.getFilterId()))) {
        desired_tag_keys = FilterUtils.desiredTagKeys(query.getFilter(tsdc.getFilterId()));
      } else if (tsdc.getFilter() != null) {
        desired_tag_keys = FilterUtils.desiredTagKeys(tsdc.getFilter());
      }
      
      if (desired_tag_keys == null) {
        desired_tag_keys = Sets.newHashSet();
      }
      findGroupByTags(graph, tsdc, desired_tag_keys);
      
      // now, see if we match an agg.
      TagsAndAggs tags_and_aggs = metric_pattern.matchingTagsAndAggs(desired_tag_keys);
      if (tags_and_aggs == null) {
        tsdc = rebuildRaw(tsdc, rebuilt);
        rebuild = true;
        continue;
      }
      
      Integer agg_start = tags_and_aggs.getAggs().get(agg);
      if (agg_start == null) {
        tsdc = rebuildRaw(tsdc, rebuilt);
        rebuild = true;
        continue;
      }
      
      // check timestamp first. If the query is too early then we can't change
      // it.
      if (query.startTime().epoch() < agg_start) {
        tsdc = rebuildRaw(tsdc, rebuilt);
        rebuild = true;
        continue;
      }
      
      if (tsdc.timeShifts() != null) {
        final TimeStamp ts = query.startTime().getCopy();
        // TODO - previous or next, figure it out.
        ts.add((TemporalAmount) tsdc.timeShifts().getValue()); 
        if (ts.epoch() < agg_start) {
          tsdc = rebuildRaw(tsdc, rebuilt);
          rebuild = true;
          continue;
        }
      }
      
      // matched!
      rebuild = true;
      if (!(Strings.isNullOrEmpty(tsdc.getFilterId()))) {
        // re-write
        if (rebuilt == null) {
          rebuilt = Maps.newHashMap();
        }
        tsdc = (TimeSeriesDataSourceConfig) 
            ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
            .setFilterId(null)
            .setQueryFilter(rebuildFilter(
                query.getFilter(tsdc.getFilterId()), agg.toUpperCase(), 
                desired_tag_keys, tags_and_aggs.getTags()))
            .build();
        rebuilt.put(tsdc.getId(), tsdc);
      } else {
        // re-write
        if (rebuilt == null) {
          rebuilt = Maps.newHashMap();
        }
        tsdc = (TimeSeriesDataSourceConfig) 
            ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
            .setQueryFilter(rebuildFilter(tsdc.getFilter(), agg.toUpperCase(),
                desired_tag_keys, tags_and_aggs.getTags()))
            .build();
        rebuilt.put(tsdc.getId(), tsdc);
      }
    }
    
    if (rebuild) {
      if (builder == null) {
        builder = ((SemanticQuery) query).toBuilder();
      }
      
      List<QueryNodeConfig> new_configs = Lists.newArrayList();
      for (final QueryNodeConfig extant : query.getExecutionGraph()) {
        if (!rebuilt.containsKey(extant.getId())) {
          new_configs.add(extant);
        }
      }
      
      new_configs.addAll(rebuilt.values());
      builder.setExecutionGraph(new_configs);
    }
    return builder;
  }
  
  SemanticQuery.Builder filterCacheMode(final SemanticQuery.Builder upstream_builder,
                                        final TimeSeriesQuery query) {
    SemanticQuery.Builder builder = upstream_builder;
    if (query.getCacheMode() == null || 
        (builder != null && builder.getCacheMode() == null)) {
      final String default_cache_mode = tsdb.getConfig().getString(DEFAULT_CACHE_MODE);
      if (!Strings.isNullOrEmpty(default_cache_mode)) {
        final CacheMode mode = CacheMode.valueOf(default_cache_mode);
        if (builder == null) {
          builder = ((SemanticQuery) query).toBuilder();
          builder.setCacheMode(mode);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Overriding cache mode for query to " + mode);
          }
        } else {
          builder.setCacheMode(mode);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Overriding cache mode for query to " + mode);
          }
        }
      }
    }
    return builder;
  }
  
  SemanticQuery.Builder filterMetrics(final SemanticQuery.Builder upstream_builder,
                                      final TimeSeriesQuery query) {
    SemanticQuery.Builder builder = upstream_builder;
    MetricFilters metric_filters = 
        tsdb.getConfig().getTyped(METRIC_KEY, MetricFilters.class);
    if (metric_filters == null) {
      return builder;
    }
    
    final List<QueryNodeConfig> current_graph = builder != null ? 
        builder.executionGraph() : query.getExecutionGraph();
    List<QueryNodeConfig> new_executions = null;
    for (int i = 0; i < current_graph.size(); i++) {
      QueryNodeConfig config = current_graph.get(i);
      if (config.getType().equals(TimeSeriesDataSourceConfig.DEFAULT) ||
          config instanceof TimeSeriesDataSourceConfig) {
        // TODO - will break if we have diff metric filters.
        if (((TimeSeriesDataSourceConfig) config).getMetric() == null) {
          continue;
        }
        
        final String metric = ((TimeSeriesDataSourceConfig) config).getMetric().getMetric();
        
        // TODO - this is a skiplist map so we could, if we have a prefix in the
        // regex, skip a bunch of entries if this is a big list of entries. For
        // now we just iterate but we can look for way of loading the least common
        // denominator of the prefixes.
        for (final Entry<MetricFilter, Map<String, String>> entry : metric_filters.entrySet()) {
          if (entry.getKey().matches(metric)) {
            QueryNodeConfig.Builder node_builder = null;
            Map<String, String> filters = entry.getValue();
            String rollup_override = filters.get(FILTER_ROLLUP);
            if (!Strings.isNullOrEmpty(rollup_override)) {
              node_builder = config.toBuilder();
              node_builder.addOverride("tsd.query.rollups.default_usage", rollup_override);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Overriding rollup for metric [" + metric 
                    + "] matching pattern /" + entry.getKey().metric + "/ to " 
                    + rollup_override);
              }
            }
            
            String byte_limit = filters.get(FILTER_BYTE_LIMIT);
            if (!Strings.isNullOrEmpty(byte_limit)) {
              if (node_builder == null) {
                node_builder = config.toBuilder();
              }
              node_builder.addOverride(Schema.QUERY_BYTE_LIMIT_KEY, byte_limit);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Overriding byte limit for metric [" + metric 
                    + "] matching pattern /" + entry.getKey().metric + "/ to " 
                    + byte_limit);
              }
            }
            
            if (node_builder != null) {
              if (new_executions == null) {
                new_executions = Lists.newArrayList(query.getExecutionGraph());
              }
              new_executions.set(i, node_builder.build());
            }
            
            break;
          }
        }
      }
    }
    
    if (new_executions != null) {
      if (builder == null) {
        builder = ((SemanticQuery) query).toBuilder();
      }
      builder.setExecutionGraph(new_executions);
    }
    return builder;
  }
  
  TimeSeriesDataSourceConfig rebuildRaw(final TimeSeriesDataSourceConfig tsdc, 
                                        final Map<String, QueryNodeConfig> rebuilt) {
    TimeSeriesDataSourceConfig new_tsdc = (TimeSeriesDataSourceConfig) 
        ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
        .setQueryFilter(rebuildFilter(tsdc.getFilter(), "raw"))
        .build();
    rebuilt.put(new_tsdc.getId(), new_tsdc);
    return new_tsdc;
  }
  
  void findGroupByTags(final MutableGraph<QueryNodeConfig> graph,
                       final QueryNodeConfig config,
                       final Set<String> tag_keys) {
    if (config instanceof GroupByConfig) {
      tag_keys.addAll(((GroupByConfig) config).getTagKeys());
    }
    
    // keep going as we may have multiple group bys.
    for (final QueryNodeConfig pred : graph.predecessors(config)) {
      findGroupByTags(graph, pred, tag_keys);
    }
  }
  
  String findAgg(final String id, 
                 final MutableGraph<QueryNodeConfig> graph, 
                 final QueryNodeConfig config) {
    return findAgg(graph, config);
  }
  
  String findAgg(final MutableGraph<QueryNodeConfig> graph, 
                 final QueryNodeConfig config) {
    if (config instanceof GroupByConfig) {
      return ((GroupByConfig) config).getAggregator();
    }
    for (final QueryNodeConfig pred : graph.predecessors(config)) {
      final String agg = findAgg(graph, pred);
      if (!Strings.isNullOrEmpty(agg)) {
        return agg.toUpperCase();
      }
    }
    return null;
  }
  
  boolean hasAgg(final QueryFilter filter) {
    if (filter instanceof TagValueFilter) {
      if (((TagValueFilter) filter).getTagKey().equals("_aggregate")) {
        return true;
      }
    }
    
    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub : (((ChainFilter) filter).getFilters())) {
        if (hasAgg(sub)) {
          return true;
        }
      }
    }
    
    if (filter instanceof NestedQueryFilter) {
      return hasAgg(((NestedQueryFilter) filter).getFilter());
    }
    return false;
  }
  
  boolean hasTagKey(final QueryFilter filter, final List<String> keys) {
    if (filter instanceof TagValueFilter) {
      if (keys.contains(((TagValueFilter) filter).getTagKey())) {
        return true;
      }
    }
    
    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub : (((ChainFilter) filter).getFilters())) {
        if (hasAgg(sub)) {
          return true;
        }
      }
    }
    
    if (filter instanceof NestedQueryFilter) {
      return hasAgg(((NestedQueryFilter) filter).getFilter());
    }
    return false;
  }
  
  // TODO - these two don't handle all cases like ORs or multiple nestings, etc.
  QueryFilter rebuildFilter(final QueryFilter filter, 
                            final String agg,
                            final Set<String> desired_tag_keys,
                            final List<String> pre_agg_keys) {
    
    QueryFilter rebuilt_filter = rebuildFilter(filter, agg);
    if (!tagSetsMatch(desired_tag_keys, pre_agg_keys)) {
      ChainFilter.Builder builder = ChainFilter.newBuilder()
          .addFilter(rebuilt_filter);
      for (int i = 0; i < pre_agg_keys.size(); i++) {
        if (!desired_tag_keys.contains(pre_agg_keys.get(i))) {
          builder.addFilter(TagValueWildcardFilter.newBuilder()
              .setFilter("*")
              .setKey(pre_agg_keys.get(i))
              .build());
        }
      }
      rebuilt_filter = builder.build();
    }
    
    // TODO - ideally, look for a dupe and avoid it if possible
    return ExplicitTagsFilter.newBuilder()
        .setFilter(rebuilt_filter)
        .build();
  }
  
  QueryFilter rebuildFilter(final QueryFilter filter, final String agg) {
    if (filter == null) {
      return TagValueLiteralOrFilter.newBuilder()
          .setFilter(agg)
          .setKey("_aggregate")
          .build();
    }
    if (filter instanceof ChainFilter) {
      return rebuildFilter((ChainFilter) filter, agg);
    }
    if (filter instanceof NestedQueryFilter) {
      return rebuildFilter(((NestedQueryFilter) filter).getFilter(), agg);
    }
    return null;
  }
  
  QueryFilter rebuildFilter(final ChainFilter filter, final String agg) {
    final ChainFilter.Builder builder = ChainFilter.newBuilder()
        .setOp(filter.getOp());
    builder.addFilter(TagValueLiteralOrFilter.newBuilder()
        .setFilter(agg)
        .setKey("_aggregate")
        .build());
    for (final QueryFilter sub : filter.getFilters()) {
      builder.addFilter(sub);
    }
    return builder.build();
  }
  
  boolean tagSetsMatch(final Set<String> desired_tag_keys, List<String> pre_agg_keys) {
    if (desired_tag_keys.size() != pre_agg_keys.size()) {
      return false;
    }
    
    for (int i = 0; i < pre_agg_keys.size(); i++) {
      if (!desired_tag_keys.contains(pre_agg_keys.get(i))) {
        return false;
      }
    }
    
    return true;
  }
  
  static MutableGraph<QueryNodeConfig> buildGraph(final TimeSeriesQuery query) {
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false)
        .build();
    Map<String, QueryNodeConfig> map = Maps.newHashMap();
    for (final QueryNodeConfig config : query.getExecutionGraph()) {
      graph.addNode(config);
      map.put(config.getId(), config);
    }
    for (final QueryNodeConfig config : query.getExecutionGraph()) {
      if (config.getSources() != null) {
        for (final Object source : config.getSources()) {
          graph.putEdge(config, map.get((String) source));
        }
      }
    }
    return graph;
  }
  
  @JsonDeserialize(keyUsing = MetricFilterDeserializer.class)
  public static class MetricFilters extends 
    ConcurrentSkipListMap<MetricFilter, Map<String, String>> {
    
  }
  
  public static class MetricFilter implements Comparable<MetricFilter> {
    private final String metric;
    private final Pattern metric_pattern;
    
    public MetricFilter(final String metric) {
      this.metric = metric;
      metric_pattern = Pattern.compile(metric);
    }
    
    public String getMetric(final String metric) {
      return metric;
    }
    
    public boolean matches(final String metric) {
      metric_pattern.hashCode();
      return metric_pattern.matcher(metric).find();
    }
        
    @Override
    public int hashCode() {
      return metric.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
      if (obj == null || !(obj instanceof MetricFilter)) {
        return false;
      }
      
      return ((MetricFilter) obj).metric.equals(metric);
    }

    @Override
    public int compareTo(final MetricFilter o) {
      // TODO - proper ordering of the regex with prefixes first, then wildcards.
      return o.metric.compareTo(metric);
    }

  }
  
  public static class MetricFilterDeserializer extends KeyDeserializer {
    @Override
    public Object deserializeKey(final String key, 
                                 final DeserializationContext ctxt) throws IOException {
      return new MetricFilter(key);
    }
  }
}