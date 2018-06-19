//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.utils.JSON;

/**
 * A generic query object that allows the construction of a complete DAG
 * to fetch, execute and serialize a query. This can be deserialized from
 * a JSON or YAML config for power users, otherwise it should be populated
 * from a user-friendly DSL.
 * 
 * @since 3.0
 */
public class SemanticQuery implements TimeSeriesQuery {

  /** The non-null and non-empty execution graph to build the query from. */
  private ExecutionGraph execution_graph;
  
  /** A list of sink configurations. */
  private List<QuerySinkConfig> sink_configs;
  
  /** A list of sinks to match the configs. */
  private List<QuerySink> sinks;
  
  /** An optional map of filter IDs to the filters. */
  private Map<String, Filter> filters;
  
  /** The execution mode of the query. */
  private QueryMode mode;
  
  /** The serialization options. */
  private List<SerdesOptions> serdes_options;
  
  SemanticQuery(final Builder builder) {
    execution_graph = builder.execution_graph;
    sink_configs = builder.sink_configs;
    sinks = builder.sinks;
    if (builder.filters != null) {
      filters = Maps.newHashMap();
      for (final Filter filter : builder.filters) {
        filters.put(filter.getId(), filter);
      }
    } else {
      filters = null;
    }
    
    mode = builder.mode;
    serdes_options = builder.serdes_options;
    
    // set the query if needed
    for (final ExecutionGraphNode node : execution_graph.getNodes()) {
      if (node.getConfig() != null && 
          node.getConfig() instanceof QuerySourceConfig &&
          ((QuerySourceConfig) node.getConfig()).getQuery() == null) {
        ((QuerySourceConfig) node.getConfig()).setTimeSeriesQuery(this);
      }
    }
    for (final QueryNodeConfig config : execution_graph.nodeConfigs().values()) {
      if (config instanceof QuerySourceConfig &&
          ((QuerySourceConfig) config).getQuery() == null) {
        ((QuerySourceConfig) config).setTimeSeriesQuery(this);
      }
    }
  }
  
  public ExecutionGraph getExecutionGraph() {
    return execution_graph;
  }
  
  public List<QuerySinkConfig> getSinkConfigs() {
    return sink_configs;
  }
  
  public List<QuerySink> getSinks() {
    return sinks;
  }
  
  public List<Filter> getFilters() {
    return Lists.newArrayList(filters.values());
  }
  
  public QueryMode getMode() {
    return mode;
  }
  
  public List<SerdesOptions> getSerdesOptions() {
    return serdes_options;
  }
  
  public Filter getFilter(final String filter_id) {
    return filters == null ? null : filters.get(filter_id);
  }
  
  @Override
  public int compareTo(TimeSeriesQuery o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION()
        .newHasher()
        .putBoolean(true)
        .hash();
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private ExecutionGraph execution_graph;
    private List<QuerySinkConfig> sink_configs;
    private List<QuerySink> sinks;
    private List<Filter> filters;
    private QueryMode mode;
    private List<SerdesOptions> serdes_options;
    
    public Builder setExecutionGraph(final ExecutionGraph execution_graph) {
      this.execution_graph = execution_graph;
      return this;
    }
    
    public Builder setSinkConfigs(final List<QuerySinkConfig> sink_configs) {
      this.sink_configs = sink_configs;
      return this;
    }
    
    public Builder addSinkConfig(final QuerySinkConfig sink) {
      if (sink_configs == null) {
        sink_configs = Lists.newArrayList();
      }
      sink_configs.add(sink);
      return this;
    }
    
    public Builder setSinks(final List<QuerySink> sinks) {
      this.sinks = sinks;
      return this;
    }
    
    public Builder addSink(final QuerySink sink) {
      if (sinks == null) {
        sinks = Lists.newArrayList();
      }
      sinks.add(sink);
      return this;
    }
    
    public Builder setFilters(final List<Filter> filters) {
      this.filters = filters;
      return this;
    }
    
    public Builder addFilter(final Filter filter) {
      if (filters == null) {
        filters = Lists.newArrayList();
      }
      filters.add(filter);
      return this;
    }
    
    public Builder setMode(final QueryMode mode) {
      this.mode = mode;
      return this;
    }
    
    public Builder setSerdesOptions(final List<SerdesOptions> serdes_options) {
      this.serdes_options = serdes_options;
      return this;
    }
    
    public SemanticQuery build() {
      return new SemanticQuery(this);
    }
  }

  public static Builder parse(final TSDB tsdb, final JsonNode root) {
    if (root == null) {
      throw new IllegalArgumentException("Root cannot be null.");
    }
    
    final Builder builder = newBuilder();
    JsonNode node = root.get("executionGraph");
    if (node == null) {
      throw new IllegalArgumentException("Need a graph!");
    }
    builder.setExecutionGraph(ExecutionGraph.parse(tsdb, node).build());
    
    node = root.get("filters");
    if (node != null) {
      for (final JsonNode filter : node) {
        try {
          builder.addFilter(JSON.getMapper().treeToValue(filter, Filter.class));
        } catch (JsonProcessingException e) {
          throw new IllegalStateException("Failed to parse query", e);
        }
      }
    }
    
    node = root.get("mode");
    if (node != null) {
      try {
        builder.setMode(JSON.getMapper().treeToValue(node, QueryMode.class));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Failed to parse query", e);
      }
    } else {
      builder.setMode(QueryMode.SINGLE);
    }
    
    return builder;
  }
}
