// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.expressions.ExpressionParser;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.utils.JSON;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pojo builder class used for serdes of the expression query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = TimeSeriesQuery.Builder.class)
public class TimeSeriesQuery extends Validatable implements Comparable<net.opentsdb.query.TimeSeriesQuery>, 
    net.opentsdb.query.TimeSeriesQuery {
  /** An optional name for the query */
  private String name;
  
  /** The timespan component of the query */
  private Timespan time;
  
  /** A list of filters */
  private List<Filter> filters;
  
  /** A mapping of the filters for key/value access. */
  private Map<String, Filter> filter_map;
  
  /** A list of metrics */
  private List<Metric> metrics;
  
  /** A list of expressions */
  private List<Expression> expressions;
  
  /** A list of outputs */
  private List<Output> outputs;
  
  /** The order of a sub query in a slice of queries. */
  private int order;

  /** TODO - temp: used to store the time series group ID for sub queries. Not
   * to be added to comparator, equals, builder, etc. */
  private TimeSeriesGroupId group_id;
  
  /** TODO - temp: A list for creating a query graph. */
  private List<TimeSeriesQuery> sub_queries;
  
  /** TEMP - try to find a better way. Holds a list of key/value settings. */
  private Map<String, String> config;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public TimeSeriesQuery(final Builder builder) {
    this.name = builder.name;
    this.time = builder.time;
    this.filters = builder.filters;
    this.metrics = builder.metrics;
    this.expressions = builder.expressions;
    this.outputs = builder.outputs;
    this.order = builder.order;
    this.config = builder.config;
    
    if (builder.filters != null) {
      filter_map = Maps.newHashMapWithExpectedSize(filters.size());
      for (final Filter filter : filters) {
        filter_map.put(filter.getId(), filter);
      }
    }
  }

  /** @return an optional name for the query */
  public String getName() {
    return name;
  }

  /** @return the timespan component of the query */
  public Timespan getTime() {
    return time;
  }

  /** @return a list of filters */
  public List<Filter> getFilters() {
    return filters == null ? null : Collections.unmodifiableList(filters);
  }

  /**
   * Fetches the given filter ID from the map if it exists.
   * @param id A non-null and non-empty filter Id.
   * @return Null if no filters were set or the filter ID exists, the filter if
   * present.
   */
  @JsonIgnore
  public Filter getFilter(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("Filter ID cannot be null or empty.");
    }
    if (filter_map == null) {
      return null;
    }
    return filter_map.get(id);
  }
  
  /** @return a list of metrics */
  public List<Metric> getMetrics() {
    return metrics == null ? null : Collections.unmodifiableList(metrics);
  }

  /** @return a list of expressions */
  public List<Expression> getExpressions() {
    return expressions == null ? null : Collections.unmodifiableList(expressions);
  }

  /** @return a list of outputs */
  public List<Output> getOutputs() {
    return outputs == null ? null : Collections.unmodifiableList(outputs);
  }

  /** @return The order of a aub query in a slice of queries. */
  public int getOrder() {
    return order;
  }
  
  /** @return A new builder for the query */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an query into a new builder.
   * @param query A non-null query to pull values from
   * @return A new builder populated with values from the given query.
   * @throws IllegalArgumentException if the query was null.
   * @since 3.0
   */
  public static Builder newBuilder(final TimeSeriesQuery query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    final Builder builder = new Builder()
        .setName(query.getName())
        .setTime(query.time)
        .setOrder(query.order);
    if (query.filters != null) {
      builder.setFilters(Lists.newArrayList(query.filters));
    }
    if (query.metrics != null) {
      builder.setMetrics(Lists.newArrayList(query.metrics));
    }
    if (query.expressions != null) {
      builder.setExpressions(Lists.newArrayList(query.expressions));
    }
    if (query.outputs != null) {
      builder.setOutputs(Lists.newArrayList(query.outputs));
    }
    return builder;
  }
  
  /** Validates the query
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (time == null) {
      throw new IllegalArgumentException("missing time");
    }

    validatePOJO(time, "time");

    if (metrics == null || metrics.isEmpty()) {
      throw new IllegalArgumentException("missing or empty metrics");
    }

    final Set<String> variable_ids = new HashSet<String>();
    for (Metric metric : metrics) {
      if (variable_ids.contains(metric.getId())) {
        throw new IllegalArgumentException("duplicated metric id: "
            + metric.getId());
      }
      variable_ids.add(metric.getId());
    }

    final Set<String> filter_ids = new HashSet<String>();

    if (filters != null && !filters.isEmpty()) {
      for (Filter filter : filters) {
        if (filter_ids.contains(filter.getId())) {
          throw new IllegalArgumentException("duplicated filter id: "
              + filter.getId());
        }
        filter_ids.add(filter.getId());
      }
    }
    
    if (expressions != null && !expressions.isEmpty()) {
      for (Expression expression : expressions) {
        if (variable_ids.contains(expression.getId())) {
          throw new IllegalArgumentException("Duplicated variable or expression id: "
              + expression.getId());
        }
        variable_ids.add(expression.getId());
      }
    }

    validateCollection(metrics, "metric");

    if (filters != null) {
      validateCollection(filters, "filter");
    }

    if (expressions != null) {
      validateCollection(expressions, "expression");
    }

    validateFilters();
    
    if (expressions != null) {
//      validateCollection(expressions, "expression");
//      for (final Expression exp : expressions) {
//        if (exp.getVariables() == null) {
//          throw new IllegalArgumentException("No variables found for an "
//              + "expression?! " + JSON.serializeToString(exp));
//        }
//        
//        for (final String var : exp.getVariables()) {
//          if (!variable_ids.contains(var)) {
//            throw new IllegalArgumentException("Expression [" + exp.getExpr() 
//              + "] was missing input " + var);
//          }
//        }
//      }
    }
  }

  /** Validates the filters, making sure each metric has a filter
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  private void validateFilters() {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    
    Set<String> ids = new HashSet<String>();
    for (Filter filter : filters) {
      ids.add(filter.getId());
    }

    for(Metric metric : metrics) {
      if (metric.getFilter() != null && 
          !metric.getFilter().isEmpty() && 
          !ids.contains(metric.getFilter())) {
        throw new IllegalArgumentException(
            String.format("unrecognized filter id %s in metric %s",
                metric.getFilter(), metric.getId()));
      }
    }
  }
  
  /**
   * Makes sure the ID has only letters and characters
   * @param id The ID to parse
   * @throws IllegalArgumentException if the ID is invalid
   */
  public static void validateId(final String id) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("The ID cannot be null or empty");
    }
    for (int i = 0; i < id.length(); i++) {
      final char c = id.charAt(i);
      if (!(Character.isLetterOrDigit(c))) {
        throw new IllegalArgumentException("Invalid id (\"" + id + 
            "\"): illegal character: " + c);
      }
    }
    if (id.length() == 1) {
      if (Character.isDigit(id.charAt(0))) {
        throw new IllegalArgumentException("The ID cannot be an integer");
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    TimeSeriesQuery query = (TimeSeriesQuery) o;

    return Objects.equal(query.expressions, expressions)
        && Objects.equal(query.filters, filters)
        && Objects.equal(query.metrics, metrics)
        && Objects.equal(query.name, name)
        && Objects.equal(query.outputs, outputs)
        && Objects.equal(query.time, time)
        && Objects.equal(query.config,  config);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode local_hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(name), Const.UTF8_CHARSET)
        .hash();
    final List<HashCode> hashes = 
        Lists.newArrayListWithCapacity(2 + 
            (filters != null ? filters.size() : 0) + 
            metrics.size() +
            (expressions != null ? expressions.size() : 0) +
            (outputs != null ? outputs.size() : 0) + 
            (config != null ? config.size() : 0));
    hashes.add(local_hc);
    if (time != null) {
      hashes.add(time.buildHashCode());
    }
    if (filters != null) {
      for (final Filter filter : filters) {
        hashes.add(filter.buildHashCode());
      }
    }
    for (final Metric metric : metrics) {
      hashes.add(metric.buildHashCode());
    }
    if (expressions != null) {
      for (final Expression exp : expressions) {
        hashes.add(exp.buildHashCode());
      }
    }
    if (outputs != null) {
      for (final Output output : outputs) {
        hashes.add(output.buildHashCode());
      }
    }
    if (config != null) {
      final List<String> keys = Lists.newArrayList(config.keySet());
      Collections.sort(keys);
      final Hasher hasher = Const.HASH_FUNCTION().newHasher();
      for (final String key : keys) {
        hasher.putString(key + config.get(key), Const.UTF8_CHARSET);
      }
      hashes.add(hasher.hash());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing  hashing without
   * the timestamps. */
  public HashCode buildTimelessHashCode() {
    final HashCode local_hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(name), Const.UTF8_CHARSET)
        .hash();
    final List<HashCode> hashes = 
        Lists.newArrayListWithCapacity(2 + 
            (filters != null ? filters.size() : 0) + 
            metrics.size() +
            (expressions != null ? expressions.size() : 0) +
            (outputs != null ? outputs.size() : 0));
    hashes.add(local_hc);
    if (time != null) {
      hashes.add(time.buildTimelessHashCode());
    }
    if (filters != null) {
      for (final Filter filter : filters) {
        hashes.add(filter.buildHashCode());
      }
    }
    for (final Metric metric : metrics) {
      hashes.add(metric.buildHashCode());
    }
    if (expressions != null) {
      for (final Expression exp : expressions) {
        hashes.add(exp.buildHashCode());
      }
    }
    if (outputs != null) {
      for (final Output output : outputs) {
        hashes.add(output.buildHashCode());
      }
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final net.opentsdb.query.TimeSeriesQuery o) {
    if (!(o instanceof TimeSeriesQuery)) {
      return -1;
    }
    ComparisonChain chain = ComparisonChain.start()
        .compare(name, ((TimeSeriesQuery) o).name, Ordering.natural().nullsFirst())
        .compare(time, ((TimeSeriesQuery) o).time, Ordering.natural().nullsFirst())
        .compare(filters, ((TimeSeriesQuery) o).filters, 
            Ordering.<Filter>natural().lexicographical().nullsFirst())
        .compare(metrics, ((TimeSeriesQuery) o).metrics, 
            Ordering.<Metric>natural().lexicographical().nullsFirst())
        .compare(expressions, ((TimeSeriesQuery) o).expressions, 
            Ordering.<Expression>natural().lexicographical().nullsFirst())
        .compare(outputs, ((TimeSeriesQuery) o).outputs, 
            Ordering.<Output>natural().lexicographical().nullsFirst());
    if (config != null) {
      final List<String> keys = Lists.newArrayList(config.keySet());
      Collections.sort(keys);
      for (final String key : keys) {
        chain = chain.compare(config.get(key), 
            ((TimeSeriesQuery) o).config == null ? 
                null : ((TimeSeriesQuery) o).config.get(key), 
                  Ordering.natural().nullsFirst());
      }
    }
    return chain.result();
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
  
  public TimeSeriesGroupId groupId() {
    return group_id;
  }
  
  public void groupId(final TimeSeriesGroupId id) {
    group_id = id;
  }
  
  public void addSubQuery(final TimeSeriesQuery query) {
    if (sub_queries == null) {
      sub_queries = Lists.newArrayListWithExpectedSize(1);
    }
    sub_queries.add(query);
  }
  
  public List<TimeSeriesQuery> subQueries() {
    return sub_queries == null ? Collections.<TimeSeriesQuery>emptyList() :
      Collections.unmodifiableList(sub_queries);
  }
  
  public Map<String, String> getConfig() {
    return config;
  }
  
  /**
   * Retrieve a query-time override as a string.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The string or null if not set anywhere.
   */
  public String getString(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = this.config == null ? null : this.config.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getString(key);
      }
    }
    return value;
  }
  
  /**
   * Retrieve a query-time override as an integer.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The string or null if not set anywhere.
   */
  public int getInt(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = this.config == null ? null : this.config.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Integer.parseInt(value);
  }
  
  /**
   * Retrieve a query-time override as a boolean. Only 'true', '1' or 'yes'
   * are considered true.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The string or null if not set anywhere.
   */
  public boolean getBoolean(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = this.config == null ? null : this.config.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getBoolean(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    value = value.toLowerCase();
    return value.equals("true") || value.equals("1") || value.equals("yes");
  }
  
  /**
   * Whether or not the key is present in the map, may have a null value.
   * @param key The non-null and non-empty key.
   * @return True if the key is present.
   */
  public boolean hasKey(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return config == null ? false : config.containsKey(key);
  }
  
  /**
   * Converts the query into a semantic query builder. 
   * @return A non-null builder if successful.
   */
  public SemanticQuery.Builder convert() {
    validate();
    
    final SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE);
    
    if (filters != null) {
      builder.setFilters(filters);
    }
    
    Map<String, String> id_to_node_roots = Maps.newHashMap();
    
    final List<ExecutionGraphNode> nodes = Lists.newArrayList();
    for (final Metric metric : metrics) {
      ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
          .setId(metric.getId())
          .setType("DataSource")
          .setConfig(QuerySourceConfig.newBuilder()
              .setStart(time.getStart())
              .setEnd(time.getEnd())
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric(metric.getMetric())
                  .build())
              .setFilterId(metric.getFilter())
              .build())
          .build();
      nodes.add(node);
      
      final String interpolator;
      String agg = !Strings.isNullOrEmpty(metric.getAggregator()) ?
          metric.getAggregator().toLowerCase() : 
            time.getAggregator().toLowerCase();
      if (agg.contains("zimsum") || 
          agg.contains("mimmax") ||
          agg.contains("mimmin")) {
        interpolator = null;
      } else {
        interpolator = "LERP";
      }
      
      // downsampler
      final Downsampler downsampler = metric.getDownsampler() != null ? 
          metric.getDownsampler() : time.getDownsampler();
      if (downsampler != null) {
        node = downsampler(metric, interpolator, downsampler);
        nodes.add(node);
      }
      
      if (metric.isRate()) {
        node = rate(metric, metric.getRateOptions(), node);
        nodes.add(node);
      } else if (time.isRate()) {
        node = rate(metric, time.getRateOptions(), node);
        nodes.add(node);
      }
      
      final Filter filter = Strings.isNullOrEmpty(metric.getFilter()) ? null 
          : getFilter(metric.getFilter());
      ExecutionGraphNode gb = groupBy(metric, interpolator, downsampler, filter, node);
      if (gb != null) {
        nodes.add(gb);
        node = gb;
      }
      
      id_to_node_roots.put(metric.getId(), node.getId());
    }
    
    if (expressions != null) {
      for (final Expression expression : expressions) {
        
        final Set<String> variables = ExpressionParser.parseVariables(expression.getExpr());
        final List<String> vars = Lists.newArrayListWithExpectedSize(variables.size());
        for (final String var : variables) {
          vars.add(id_to_node_roots.get(var));
        }
        
        // TODO - figure out query tags to mimic 2.x
        // TODO - get fills from metrics
        ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
            .setId(expression.getId())
            .setType("Expression")
            .setConfig(ExpressionConfig.newBuilder()
                .setExpression(expression.getExpr())
                //.setAs("") // TODO ??
                .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                    .setType(JoinType.NATURAL)
                    .build())
                .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NONE) // TODO
                    .setRealFillPolicy(FillWithRealPolicy.NONE)
                    .setId("LERP")
                    .setType(NumericType.TYPE.toString())
                    .build())
                .setId(expression.getId())
                .build())
            .setSources(vars)
            .build();
        nodes.add(node);
      }
    }
    
    builder.setExecutionGraph(ExecutionGraph.newBuilder()
        .setNodes(nodes)
        .setId(name)
        .build());
    return builder;
  }
  
  /**
   * Helper to build the downsampler node.
   * @param metric A non-null metric.
   * @param interpolator The name of the interpolator to use based on 
   * the aggegation.
   * @param downsampler The downsampler to pull config from.
   * @return An execution graph node.
   */
  private ExecutionGraphNode downsampler(final Metric metric, 
                                 final String interpolator, 
                                 final Downsampler downsampler) {
    
    FillPolicy policy = downsampler.getFillPolicy().getPolicy();
    
    DownsampleConfig.Builder ds = (DownsampleConfig.Builder) DownsampleConfig.newBuilder()
        .setAggregator(downsampler.getAggregator())
        .setInterval(downsampler.getInterval())
        .setFill(downsampler.getFillPolicy() != null ? true : false)
        .setId("downsample_" + metric.getId())
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                  .setFillPolicy(policy)
                  .setRealFillPolicy(FillWithRealPolicy.NONE)
                  .setId(interpolator)
                  .setType(NumericType.TYPE.toString())
                  .build());
    if (!Strings.isNullOrEmpty(downsampler.getTimezone())) {
      ds.setTimeZone(downsampler.getTimezone());
    }
        
    return ExecutionGraphNode.newBuilder()
        .setId(metric.getId() + "_Downsampler")
        .setType("Downsample")
        .setConfig(ds.build())
        .addSource(metric.getId())
        .build();
  }
  
  /**
   * Generates a rate node.
   * @param metric The non-null metric.
   * @param options The rate options.
   * @param parent The parent node.
   * @return An execution graph node.
   */
  private ExecutionGraphNode rate(final Metric metric, 
                          final RateOptions options, 
                          final ExecutionGraphNode parent) {
    return ExecutionGraphNode.newBuilder()
      .setId(metric.getId() + "_Rate")
      .setType("Rate")
      .addSource(parent.getId())
      .setConfig(RateOptions.newBuilder(metric.getRateOptions())
        .setId(metric.getId() + "_Rate")
        .build())
      .build();
  }
  
  /**
   * Generates a group-by node.
   * @param metric A non-null metric.
   * @param interpolator The name of the interpolator to use based on 
   * the aggegation.
   * @param downsampler The downsampler to pull config from.
   * @param filter An optional filter.
   * @param parent The parent graph node.
   * @return An execution graph node.
   */
  private ExecutionGraphNode groupBy(final Metric metric, 
                                     final String interpolator, 
                                     final Downsampler downsampler, 
                                     final Filter filter, 
                                     final ExecutionGraphNode parent) {
    final String agg = !Strings.isNullOrEmpty(metric.getAggregator()) ?
        metric.getAggregator().toLowerCase() : 
         time.getAggregator().toLowerCase();
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
                .setId(metric.getId() + "_GroupBy");
          }
          join_keys.add(v.getTagk());
        }
      }
      
      if (gb_config != null) {
        gb_config.setTagKeys(join_keys);
        gb_config.setAggregator(agg);
        return ExecutionGraphNode.newBuilder()
            .setId(metric.getId() + "_GroupBy")
            .setType("GroupBy")
            .addSource(parent.getId())
            .setConfig(gb_config.build())
            .build();
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
          .setId(metric.getId() + "_GroupBy");
          
      gb_config.setAggregator(agg);
      
      return ExecutionGraphNode.newBuilder()
          .setId(metric.getId() + "_GroupBy")
          .setType("GroupBy")
          .addSource(parent.getId())
          .setConfig(gb_config.build())
          .build();
    }
    
    return null;
  }
  
  /**
   * A builder for the query component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String name;
    @JsonProperty
    private Timespan time;
    @JsonProperty
    private List<Filter> filters;
    @JsonProperty
    private List<Metric> metrics;
    @JsonProperty
    private List<Expression> expressions;
    @JsonProperty
    private List<Output> outputs;
    @JsonProperty
    private int order;
    @JsonProperty
    private Map<String, String> config;

    public Builder() { }

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setTime(final Timespan time) {
      this.time = time;
      return this;
    }
    
    @JsonIgnore
    public Builder setTime(final Timespan.Builder time) {
      this.time = time.build();
      return this;
    }

    public Builder setFilters(final List<Filter> filters) {
      this.filters = filters;
      if (filters != null) {
        Collections.sort(this.filters);
      }
      return this;
    }

    @JsonIgnore
    public Builder addFilter(final Filter filter) {
      if (filters == null) {
        filters = Lists.newArrayList(filter);
      } else {
        filters.add(filter);
        Collections.sort(filters);
      }
      return this;
    }
    
    @JsonIgnore
    public Builder addFilter(final Filter.Builder filter) {
      if (filters == null) {
        filters = Lists.newArrayList(filter.build());
      } else {
        filters.add(filter.build());
        Collections.sort(filters);
      }
      return this;
    }
    
    public Builder setMetrics(final List<Metric> metrics) {
      this.metrics = metrics;
      if (metrics != null) {
        Collections.sort(this.metrics);
      }
      return this;
    }

    @JsonIgnore
    public Builder addMetric(final Metric metric) {
      if (metrics == null) {
        metrics = Lists.newArrayList(metric);
      } else {
        metrics.add(metric);
        Collections.sort(metrics);
      }
      return this;
    }
    
    @JsonIgnore
    public Builder addMetric(final Metric.Builder metric) {
      if (metrics == null) {
        metrics = Lists.newArrayList(metric.build());
      } else {
        metrics.add(metric.build());
        Collections.sort(metrics);
      }
      return this;
    }
    
    public Builder setExpressions(final List<Expression> expressions) {
      this.expressions = expressions;
      if (expressions != null) {
        Collections.sort(this.expressions);
      }
      return this;
    }

    @JsonIgnore
    public Builder addExpression(final Expression expression) {
      if (expressions == null) {
        expressions = Lists.newArrayList(expression);
      } else {
        expressions.add(expression);
        Collections.sort(expressions);
      }
      return this;
    }
    
    @JsonIgnore
    public Builder addExpression(final Expression.Builder expression) {
      if (expressions == null) {
        expressions = Lists.newArrayList(expression.build());
      } else {
        expressions.add(expression.build());
        Collections.sort(expressions);
      }
      return this;
    }
    
    public Builder setOutputs(final List<Output> outputs) {
      this.outputs = outputs;
      if (outputs != null) {
        Collections.sort(this.outputs);
      }
      return this;
    }

    @JsonIgnore
    public Builder addOutput(final Output output) {
      if (outputs == null) {
        outputs = Lists.newArrayList(output);
      } else {
        outputs.add(output);
        Collections.sort(outputs);
      }
      return this;
    }
    
    @JsonIgnore
    public Builder addOutput(final Output.Builder output) {
      if (outputs == null) {
        outputs = Lists.newArrayList(output.build());
      } else {
        outputs.add(output.build());
        Collections.sort(outputs);
      }
      return this;
    }
    
    public Builder setOrder(final int order) {
      this.order = order;
      return this;
    }
    
    public Builder setConfig(final Map<String, String> config) {
      this.config = config;
      return this;
    }
    
    public Builder addConfig(final String key, final String value) {
      if (Strings.isNullOrEmpty(key)) {
        throw new IllegalArgumentException("Key cannot be null.");
      }
      if (config == null) {
        config = Maps.newHashMap();
      }
      config.put(key, value);
      return this;
    }
    
    public TimeSeriesQuery build() {
      return new TimeSeriesQuery(this);
    }
  }
  
}
