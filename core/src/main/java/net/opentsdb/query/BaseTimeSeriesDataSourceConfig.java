//This file is part of OpenTSDB.
//Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 * 
 * TODO - this is ugly and needs a lot of re-org and work.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_DEFAULT)
@JsonDeserialize(builder = BaseTimeSeriesDataSourceConfig.Builder.class)
public abstract class BaseTimeSeriesDataSourceConfig extends BaseQueryNodeConfig 
    implements TimeSeriesDataSourceConfig {
  /** The source provider ID. */
  private final String source_id;
  
  // TODO - time offsets for period over period
  
  /** A list of data types to fetch. If empty, fetch all. */
  private final List<String> types;
  
  /** A non-null metric filter used to determine the metric(s) to fetch. */
  private final MetricFilter metric;
  
  /** An optional filter ID found in the query. */
  private final String filter_id;
  
  /** An optional filter. If filter_id is set, this is ignored. */
  private final QueryFilter filter;
  
  /** Whether or not to fetch only the last value. */
  private final boolean fetch_last;
  
  /** An optional list of nodes to push down to the driver. */
  private final List<QueryNodeConfig> push_down_nodes;
  
  /**
   * Private ctor for the builder.
   * @param builder The non-null builder.
   */
  protected BaseTimeSeriesDataSourceConfig(final Builder builder) {
    super(builder);
    if (builder.metric == null) {
      throw new IllegalArgumentException("Metric filter cannot be null.");
    }
    source_id = builder.sourceId;
    types = builder.types;
    metric = builder.metric;
    filter_id = builder.filterId;
    filter = builder.filter;
    fetch_last = builder.fetchLast;
    push_down_nodes = builder.push_down_nodes;
  }
  
  /** @return The source ID. May be null in which case we use the default. */
  public String getSourceId() {
    return source_id;
  }
  
  /** @return A list of data types to filter on. If null or empty, fetch
   * all. */
  public List<String> getTypes() {
    return types;
  }
  
  /** @return The non-null metric filter. */
  public MetricFilter getMetric() {
    return metric;
  }
  
  /** @return An optional filter ID to fetch. */
  public String getFilterId() {
    return filter_id;
  }
  
  /** @return The local filter if set, null if not. */
  public QueryFilter getFilter() {
    return filter;
  }
  
  /** @return Whether or not to fetch just the last (latest) value. */
  public boolean getFetchLast() {
    return fetch_last;
  }
  
  /** @return An optional list of push down nodes. May be null. */
  public List<QueryNodeConfig> getPushDownNodes() {
    return push_down_nodes;
  }
  
  @Override
  public boolean pushDown() {
    return false;
  }
  
  @Override
  public boolean joins() {
    return false;
  }
  
  @Override
  public boolean equals(final Object o) {
    // TODO Auto-generated method stub
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof BaseTimeSeriesDataSourceConfig)) {
      return false;
    }
    
    return id.equals(((BaseTimeSeriesDataSourceConfig) o).id);
  }
  
  @Override
  public int compareTo(final QueryNodeConfig o) {
    if (!(o instanceof BaseTimeSeriesDataSourceConfig)) {
      return -1;
    }
    
    // TODO - implement
    return ComparisonChain.start()
        .compare(id, ((BaseTimeSeriesDataSourceConfig) o).id, Ordering.natural().nullsFirst())
        
        .result();
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash());
    // TODO - implement in full
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A new builder. */
  public static Builder newBuilder(final TimeSeriesDataSourceConfig config,
                                   final Builder builder) {
    return (Builder) builder
        .setSourceId(config.getSourceId())
        .setTypes(config.getTypes() != null ? Lists.newArrayList(config.getTypes()) : null)
        .setMetric(config.getMetric())
        .setFilterId(config.getFilterId())
        .setQueryFilter(config.getFilter())
        .setFetchLast(config.getFetchLast())
        // TODO - overrides if we keep em.
        .setSources(config.getSources())
        .setType(config.getType())
        .setId(config.getId());
    // Skipp push down nodes.
  }

  public static void parseConfig(final ObjectMapper mapper, 
                                 final TSDB tsdb, 
                                 final JsonNode node,
                                 final Builder builder) {
    // TODO - types
    JsonNode n = node.get("sourceId");
    if (n != null && !n.isNull()) {
      builder.setSourceId(n.asText());
    }
    
    n = node.get("metric");
    if (n == null) {
      throw new IllegalArgumentException("Missing the metric field.");
    }
    JsonNode type_node = n.get("type");
    if (type_node == null) {
      throw new IllegalArgumentException("Missing the metric type field.");
    }
    String type = type_node.asText();
    if (Strings.isNullOrEmpty(type)) {
      throw new IllegalArgumentException("Metric type field cannot be null or empty.");
    }
    QueryFilterFactory factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
    if (factory == null) {
      throw new IllegalArgumentException("No query filter factory found for: " + type);
    }
    QueryFilter filter = factory.parse(tsdb, mapper, n);
    if (filter == null || !(filter instanceof MetricFilter)) {
      throw new IllegalArgumentException("Metric query filter was not "
          + "an instanceof MetricFilter: " + filter.getClass());
    }
    builder.setMetric((MetricFilter) filter);
    
    n = node.get("id");
    if (n == null || Strings.isNullOrEmpty(n.asText())) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    builder.setId(n.asText());
    
    n = node.get("fetchLast");
    if (n != null) {
      builder.setFetchLast(n.asBoolean());
    }
    
    n = node.get("filterId");
    if (n != null && !Strings.isNullOrEmpty(n.asText())) {
      builder.setFilterId(n.asText());
    } else {
      n = node.get("filter");
      if (n != null && !n.isNull()) {
        type_node = n.get("type");
        if (type_node == null) {
          throw new IllegalArgumentException("Missing the filter type field.");
        }
        
        type = type_node.asText();
        if (Strings.isNullOrEmpty(type)) {
          throw new IllegalArgumentException("Filter type field cannot be null or empty.");
        }
        
        factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
        if (factory == null) {
          throw new IllegalArgumentException("No query filter factory found for: " + type);
        }
        filter = factory.parse(tsdb, mapper, n);
        if (filter == null) {
          throw new IllegalArgumentException("Unable to parse filter config.");
        }
        builder.setQueryFilter(filter);
      }
    }
    
    n = node.get("pushDownNodes");
    if (n != null) {
      for (final JsonNode pushdown : n) {
        JsonNode temp = pushdown.get("type");
        QueryNodeFactory config_factory = null;
        if (temp != null && !temp.isNull()) {
          config_factory = tsdb.getRegistry()
              .getQueryNodeFactory(temp.asText());
        } else {
          temp = pushdown.get("id");
          if (temp != null && !temp.isNull()) {
            config_factory = tsdb.getRegistry()
                .getQueryNodeFactory(temp.asText());
          }
        }
        
        if (config_factory == null) {
          throw new IllegalArgumentException("Unable to find a config "
              + "factory for type: " + (temp == null || temp.isNull() ? 
                  "null" : temp.asText()));
        }
        builder.addPushDownNode(config_factory.parseConfig(
            mapper, tsdb, pushdown));
      }
    }
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static abstract class Builder extends BaseQueryNodeConfig.Builder
    implements TimeSeriesDataSourceConfig.Builder {
    @JsonProperty
    private String sourceId;
    @JsonProperty
    private List<String> types;
    @JsonProperty
    private MetricFilter metric;
    @JsonProperty
    private String filterId;
    @JsonProperty
    private QueryFilter filter;
    @JsonProperty
    private boolean fetchLast;
    private List<QueryNodeConfig> push_down_nodes;
    
    protected Builder() {
      setType(TimeSeriesDataSourceConfig.DEFAULT);
    }
    
    public Builder setSourceId(final String source_id) {
      sourceId = source_id;
      return this;
    }
    
    public Builder setTypes(final List<String> types) {
      this.types = types;
      return this;
    }
    
    public Builder addType(final String type) {
      if (types == null) {
        types = Lists.newArrayList();
      }
      types.add(type);
      return this;
    }
    
    public Builder setMetric(final MetricFilter metric) {
      this.metric = metric;
      return this;
    }
    
    public Builder setFilterId(final String filter_id) {
      this.filterId = filter_id;
      return this;
    }
    
    public Builder setQueryFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public Builder setFetchLast(final boolean fetch_last) {
      this.fetchLast = fetch_last;
      return this;
    }
    
    public Builder setPushDownNodes(
        final List<QueryNodeConfig> push_down_nodes) {
      this.push_down_nodes = push_down_nodes;
      return this;
    }
    
    public Builder addPushDownNode(final QueryNodeConfig node) {
      if (push_down_nodes == null) {
        push_down_nodes = Lists.newArrayList();
      }
      push_down_nodes.add(node);
      return this;
    }
    
    public abstract TimeSeriesDataSourceConfig build();
    
  }

}
