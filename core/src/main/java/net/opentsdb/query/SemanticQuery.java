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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.utils.DateTime;
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
  /** User given start date/time, could be relative or absolute */
  private final String start;
  private final TimeStamp start_ts;
  
  /** User given end date/time, could be relative, absolute or empty */
  private final String end;
  private final TimeStamp end_ts;
  
  /** User's timezone used for converting absolute human readable dates */
  private final String time_zone;
  
  /** The non-null and non-empty execution graph to build the query from. */
  private List<QueryNodeConfig> execution_graph;
  
  /** An optional map of filter IDs to the filters. */
  private Map<String, NamedFilter> filters;
  
  /** The execution mode of the query. */
  private QueryMode mode;
  
  /** The serialization options. */
  private List<SerdesOptions> serdes_options;
  
  /** The log level for this query. */
  private LogLevel log_level;
  
  SemanticQuery(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.start) && Strings.isNullOrEmpty(builder.end)) {
      throw new IllegalArgumentException("Start time is required.");
    }

    if (Strings.isNullOrEmpty(builder.start)) {
      start = null;
    } else {
      start = builder.start;
    }

    if (Strings.isNullOrEmpty(builder.end)) {
      end = null;
    } else {
      end = builder.end;
    }

    // we have end time, but no start time
    if (Strings.isNullOrEmpty(builder.start) && !Strings.isNullOrEmpty(builder.end)) { 

      // is end time before or after current time?
      if (DateTime.parseDateTimeString(end, builder.time_zone) > DateTime.currentTimeMillis()) {
        end_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(end, builder.time_zone));
        start_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      } else {
        start_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(end, builder.time_zone));
        end_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      }

    // we have start time, but no end time
    } else if (!Strings.isNullOrEmpty(builder.start) && Strings.isNullOrEmpty(builder.end)) {

      // is start time before or after current time?
      if (DateTime.parseDateTimeString(start, builder.time_zone) > DateTime.currentTimeMillis()) {
        end_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(start, builder.time_zone));
        start_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      } else {
        start_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(start, builder.time_zone));
        end_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      }

    } else if (DateTime.parseDateTimeString(start, builder.time_zone) > 
      DateTime.parseDateTimeString(end, builder.time_zone)) { // start after end
        start_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(end, builder.time_zone));
        end_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(start, builder.time_zone));

    } else { // regular input
      start_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(start, builder.time_zone));
      end_ts = new MillisecondTimeStamp(DateTime.parseDateTimeString(end, builder.time_zone));
    }

    time_zone = builder.time_zone;
    
    // TODO need checks here
    if (builder.mode == null) {
      throw new IllegalArgumentException("Mode cannot be null.");
    }
    if (builder.execution_graph == null) {
      throw new IllegalArgumentException("Execution graph cannot be null.");
    }
    execution_graph = builder.execution_graph;
    if (builder.filters != null) {
      filters = Maps.newHashMap();
      for (final NamedFilter filter : builder.filters) {
        filters.put(filter.getId(), filter);
      }
    } else {
      filters = Maps.newHashMapWithExpectedSize(0);
    }
    
    mode = builder.mode;
    serdes_options = builder.serdes_config == null ? 
        Collections.emptyList() : builder.serdes_config;
    log_level = builder.log_level;
  }

  @Override
  public String getStart() {
    return start;
  }

  @Override
  public String getEnd() {
    return end;
  }

  @Override
  public String getTimezone() {
    return time_zone;
  }
  
  @Override
  public List<QueryNodeConfig> getExecutionGraph() {
    return execution_graph;
  }
  
  @Override
  public List<SerdesOptions> getSerdesConfigs() {
    return serdes_options;
  }
  
  public List<NamedFilter> getFilters() {
    return Lists.newArrayList(filters.values());
  }
  
  @Override
  public QueryMode getMode() {
    return mode;
  }
  
  @Override
  public QueryFilter getFilter(final String filter_id) {
    if (filters == null) {
      return null;
    }
    final NamedFilter filter = filters.get(filter_id);
    if (filter == null) {
      return null;
    }
    return filter.getFilter();
  }
  
  @Override 
  public TimeStamp startTime() {
    return start_ts;
  }
  
  @Override
  public TimeStamp endTime() {
    return end_ts;
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

  @Override
  public LogLevel getLogLevel() {
    return log_level;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private String start;
    private String end;
    private String time_zone;
    private List<QueryNodeConfig> execution_graph;
    private List<NamedFilter> filters;
    private QueryMode mode;
    private List<SerdesOptions> serdes_config;
    private LogLevel log_level = LogLevel.ERROR;
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }
    
    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }
    
    public Builder setTimeZone(final String time_zone) {
      this.time_zone = time_zone;
      return this;
    }
    
    public Builder setExecutionGraph(final List<QueryNodeConfig> execution_graph) {
      this.execution_graph = execution_graph;
      return this;
    }
    
    public Builder addExecutionGraphNode(final QueryNodeConfig node) {
      if (execution_graph == null) {
        execution_graph = Lists.newArrayList();
      }
      execution_graph.add(node);
      return this;
    }
    
    public Builder setFilters(final List<NamedFilter> filters) {
      this.filters = filters;
      return this;
    }
    
    public Builder addFilter(final NamedFilter filter) {
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
    
    public Builder setSerdesConfigs(final List<SerdesOptions> serdes_config) {
      this.serdes_config = serdes_config;
      return this;
    }
    
    public Builder addSerdesConfig(final SerdesOptions serdes_config) {
      if (this.serdes_config == null) {
        this.serdes_config = Lists.newArrayList();
      }
      this.serdes_config.add(serdes_config);
      return this;
    }
    
    public Builder setLogLevel(final LogLevel log_level) {
      this.log_level = log_level;
      return this;
    }
    
    public List<SerdesOptions> serdesConfigs() {
      return serdes_config;
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
    for (final JsonNode config : node) {
      QueryNodeFactory config_factory = null;
      JsonNode temp = config.get("sourceId");
      if (temp != null && !temp.isNull()) {
        config_factory = tsdb.getRegistry()
            .getQueryNodeFactory(temp.asText());
      } else {
        temp = config.get("type");
        if (temp != null && !temp.isNull()) {
          config_factory = tsdb.getRegistry()
              .getQueryNodeFactory(temp.asText());
          // could be default data source so lets double check that.
          if (temp.asText().toLowerCase()
              .equals(TimeSeriesDataSourceConfig.DEFAULT.toLowerCase())) {
            config_factory = tsdb.getRegistry()
                .getDefaultPlugin(TimeSeriesDataSourceFactory.class);
          }
        } else {
          temp = config.get("id");
          if (temp != null && !temp.isNull()) {
            config_factory = tsdb.getRegistry()
                .getQueryNodeFactory(temp.asText());
            // could be default data source so lets double check that.
            if (temp.asText().toLowerCase()
                .equals(TimeSeriesDataSourceConfig.DEFAULT.toLowerCase())) {
              config_factory = tsdb.getRegistry()
                  .getDefaultPlugin(TimeSeriesDataSourceFactory.class);
            }
          }
        }
      }
      
      if (config_factory == null) {
        throw new IllegalArgumentException("Unable to find a config "
            + "factory for type: " + (temp == null ? "null" : temp.asText()));
      }
      builder.addExecutionGraphNode(config_factory.parseConfig(
          JSON.getMapper(), tsdb, config));
    }
    
    node = root.get("start");
    builder.setStart(node.asText());
    
    node = root.get("end");
    if (node != null && !node.isNull()) {
      builder.setEnd(node.asText());
    }
    
    node = root.get("timezone");
    if (node != null && !node.isNull()) {
      builder.setTimeZone(node.asText());
    }
    
    node = root.get("filters");
    if (node != null) {
      for (final JsonNode filter : node) {
        final JsonNode id_node = filter.get("id");
        if (id_node == null) {
          throw new IllegalArgumentException("Filter node was missing the ID.");
        }
        final String id = id_node.asText();
        if (Strings.isNullOrEmpty(id)) {
          throw new IllegalArgumentException("Filter ID cannot be null or empty.");
        }
        
        final JsonNode child = filter.get("filter");
        if (child == null) {
          throw new IllegalArgumentException("Filter child cannot be null or empty.");
        }
        final JsonNode type_node = child.get("type");
        if (type_node == null) {
          throw new IllegalArgumentException("Filter must include a type.");
        }
        final String type = type_node.asText();
        if (Strings.isNullOrEmpty(type)) {
          throw new IllegalArgumentException("Filter type cannot be null "
              + "or empty.");
        }
        final QueryFilterFactory factory = tsdb.getRegistry()
            .getPlugin(QueryFilterFactory.class, type);
        if (factory == null) {
          throw new IllegalArgumentException("No filter factory found "
              + "for type: " + type);
        }
        
        builder.addFilter(DefaultNamedFilter.newBuilder()
            .setId(id)
            .setFilter(factory.parse(tsdb, JSON.getMapper(), child))
            .build());
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
    
    node = root.get("logLevel");
    if (node != null && !node.isNull()) {
      builder.setLogLevel(LogLevel.valueOf(node.asText().toUpperCase()));
    }
    
    node = root.get("serdesConfigs");
    if (node != null) {
      for (final JsonNode serdes : node) {
        SerdesFactory factory = null;
        node = serdes.get("type");
        if (node == null || node.isNull()) {
          node = serdes.get("id");
          if (node == null || node.isNull()) {
            throw new IllegalArgumentException("The serdes config needs "
                + "a type and/or ID.");
          }
          factory = tsdb.getRegistry().getPlugin(SerdesFactory.class, 
              node.asText());
        } else {
          factory = tsdb.getRegistry().getPlugin(SerdesFactory.class, 
              node.asText());
        }
        
        if (factory == null) {
          throw new IllegalArgumentException("No serdes factory found for: " 
              + node.asText());
        }
        
        final SerdesOptions config = factory.parseConfig(
            JSON.getMapper(), tsdb, serdes);
        if (config == null) {
          throw new IllegalArgumentException("Serdes factory returned a "
              + "null config for: " + node.asText());
        }
        builder.addSerdesConfig(config);
      }
    }
    
    return builder;
  }
}
