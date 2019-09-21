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
package net.opentsdb.query;

import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.NestedQueryFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.processor.groupby.GroupByConfig;

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
  
  private static final TypeReference<Map<String, Map<String, String>>> USER_FILTERS =
      new TypeReference<Map<String, Map<String, String>>>() { };
  private static final TypeReference<
    Map<String, Map<String, Map<String, String>>>> HEADER_FILTERS =
      new TypeReference<Map<String, Map<String, Map<String, String>>>>() { };
  private static final TypeReference<Map<String, PreAggConfig>> PREAGG_FILTERS =
      new TypeReference<Map<String, PreAggConfig>>() { };
  private static final String HEADER_KEY = "tsd.queryfilter.filter.headers";
  private static final String USER_KEY = "tsd.queryfilter.filter.users";
  private static final String PREAGG_KEY = "tsd.queryfilter.filter.preagg";
  
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
          .setType(USER_FILTERS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(PREAGG_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(PREAGG_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(PREAGG_FILTERS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSeriesQuery filter(final TimeSeriesQuery query, 
      final AuthState auth_state, 
      final Map<String, String> headers) {
    SemanticQuery.Builder builder = null;
    Map<String, Map<String, Map<String, String>>> hdr_filter = 
        tsdb.getConfig().getTyped(HEADER_KEY, HEADER_FILTERS);
    if (!hdr_filter.isEmpty() && headers != null) {
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
            
            String ov = values.get("cacheMode");
            if (ov != null && query.getCacheMode() == null) {
              builder.setCacheMode(CacheMode.valueOf(ov));
              LOG.trace("Overriding cache mode for header: " + 
                  entry.getKey() + ":" + entry.getValue() + " to " + 
                  CacheMode.valueOf(ov));
            }
          }
        }
      }
    }
    
    Map<String, Map<String, String>> user_filters = 
        tsdb.getConfig().getTyped(USER_KEY, USER_FILTERS);
    if (user_filters != null) {
      final String user = auth_state != null && 
          auth_state.getPrincipal() != null ? 
              auth_state.getPrincipal().getName() : "Unknown";
      Map<String, String> filter = user_filters.get(user);
      if (filter != null) {
        // OVERRIDE the query!
        if (builder == null) {
          builder = ((SemanticQuery) query).toBuilder();
        }
        String ov = filter.get("cacheMode");
        if (ov != null && query.getCacheMode() == null) {
          builder.setCacheMode(CacheMode.valueOf(ov));
          if (LOG.isTraceEnabled()) {
            LOG.trace("Overriding cache mode for user: " + user + " to " 
                + CacheMode.valueOf(ov));
          }
        }
      }
    }
    
    // TODO - tons of stuff to do here, e.g. nulls from the group by or the 
    // filters.
    Map<String, PreAggConfig> preagg_filters = 
        tsdb.getConfig().getTyped(PREAGG_KEY, PREAGG_FILTERS);
    if (preagg_filters != null && !preagg_filters.isEmpty()) {
      Map<String, QueryNodeConfig> rebuilt = null;
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
        
        if (!(Strings.isNullOrEmpty(tsdc.getFilterId()))) {
          if (hasAgg(query.getFilter(tsdc.getFilterId()))) {
            // Nothing to modify here.
            continue;
          }
        } else if (tsdc.getFilter() != null) {
          if (hasAgg(tsdc.getFilter())) {
            // Nothing to modify here.
            continue;
          }
        }
        
        // check timestamp first. If the query is too early then we can't change
        // it.
        if (query.startTime().epoch() < preagg.startEpoch) {
          // TODO - set raw
          continue;
        }
        
        if (tsdc.timeShifts() != null) {
          final TimeStamp ts = query.startTime().getCopy();
          // TODO - previous or next, figure it out.
          ts.add((TemporalAmount) tsdc.timeShifts().getValue()); 
          if (ts.epoch() < preagg.startEpoch) {
            // TODO - set raw
            continue;
          }
        }
        
        // see if we match the tags
        if (!(Strings.isNullOrEmpty(tsdc.getFilterId()))) {
          if (hasTagKey(query.getFilter(tsdc.getFilterId()), preagg.excludedAggTags)) {
            if (rebuilt == null) {
              rebuilt = Maps.newHashMap();
            }
            tsdc = (TimeSeriesDataSourceConfig) 
                ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
                .setQueryFilter(rebuildFilter(tsdc.getFilter(), "raw"))
                .build();
            rebuilt.put(tsdc.getId(), tsdc);
            continue;
          }
        } else if (tsdc.getFilter() != null && 
                   hasTagKey(tsdc.getFilter(), preagg.excludedAggTags)) {
          if (rebuilt == null) {
            rebuilt = Maps.newHashMap();
          }
          tsdc = (TimeSeriesDataSourceConfig) 
              ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
              .setQueryFilter(rebuildFilter(tsdc.getFilter(), "raw"))
              .build();
          rebuilt.put(tsdc.getId(), tsdc);
          continue;
        }
        
        rebuild = true;
        if (graph == null) {
          graph = buildGraph(query);
        }
        final String agg = findAgg(tsdc.getId(), graph, tsdc);
        if (!(Strings.isNullOrEmpty(tsdc.getFilterId()))) {
          // re-write
          if (rebuilt == null) {
            rebuilt = Maps.newHashMap();
          }
          tsdc = (TimeSeriesDataSourceConfig) 
              ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
              .setFilterId(null)
              .setQueryFilter(rebuildFilter(
                  query.getFilter(tsdc.getFilterId()), agg))
              .build();
          rebuilt.put(tsdc.getId(), tsdc);
        } else {
          // re-write
          if (rebuilt == null) {
            rebuilt = Maps.newHashMap();
          }
          tsdc = (TimeSeriesDataSourceConfig) 
              ((TimeSeriesDataSourceConfig.Builder) tsdc.toBuilder())
              .setQueryFilter(rebuildFilter(tsdc.getFilter(), agg))
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
    }
    
    return builder != null ? builder.build() : query;
  }

  @Override
  public String type() {
    return "DefaultQueryContextFilter";
  }
  
  public static class PreAggConfig {
    public int startEpoch;
    public List<String> excludedAggTags;
  }
  
  String findAgg(final String id, final MutableGraph<QueryNodeConfig> graph, final QueryNodeConfig config) {
    return findAgg(graph, config);
  }
  
  String findAgg(final MutableGraph<QueryNodeConfig> graph, final QueryNodeConfig config) {
    if (config instanceof GroupByConfig) {
      return ((GroupByConfig) config).getAggregator();
    }
    for (final QueryNodeConfig pred : graph.predecessors(config)) {
      final String agg = findAgg(graph, pred);
      if (!Strings.isNullOrEmpty(agg)) {
        return agg;
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
}