// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.utils.JSON;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pojo builder class used for serdes of the expression query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Query.Builder.class)
public class Query extends Validatable implements Comparable<Query> {
  /** An optional name for the query */
  private String name;
  
  /** The timespan component of the query */
  private Timespan time;
  
  /** A list of filters */
  private List<Filter> filters;
  
  /** A list of metrics */
  private List<Metric> metrics;
  
  /** A list of expressions */
  private List<Expression> expressions;
  
  /** A list of outputs */
  private List<Output> outputs;
  
  /** The order of a sub query in a slice of queries. */
  private int order;

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Query(final Builder builder) {
    this.name = builder.name;
    this.time = builder.time;
    this.filters = builder.filters;
    this.metrics = builder.metrics;
    this.expressions = builder.expressions;
    this.outputs = builder.outputs;
    this.order = builder.order;
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
  public static Builder newBuilder(final Query query) {
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
      validateCollection(expressions, "expression");
      for (final Expression exp : expressions) {
        if (exp.getVariables() == null) {
          throw new IllegalArgumentException("No variables found for an "
              + "expression?! " + JSON.serializeToString(exp));
        }
        
        for (final String var : exp.getVariables()) {
          if (!variable_ids.contains(var)) {
            throw new IllegalArgumentException("Expression [" + exp.getExpr() 
              + "] was missing input " + var);
          }
        }
      }
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

    Query query = (Query) o;

    return Objects.equal(query.expressions, expressions)
        && Objects.equal(query.filters, filters)
        && Objects.equal(query.metrics, metrics)
        && Objects.equal(query.name, name)
        && Objects.equal(query.outputs, outputs)
        && Objects.equal(query.time, time);
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
            (outputs != null ? outputs.size() : 0));
    hashes.add(local_hc);
    hashes.add(time.buildHashCode());
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
  public int compareTo(final Query o) {
    return ComparisonChain.start()
        .compare(name, o.name, Ordering.natural().nullsFirst())
        .compare(time, o.time, Ordering.natural().nullsFirst())
        .compare(filters, o.filters, 
            Ordering.<Filter>natural().lexicographical().nullsFirst())
        .compare(metrics, o.metrics, 
            Ordering.<Metric>natural().lexicographical().nullsFirst())
        .compare(expressions, o.expressions, 
            Ordering.<Expression>natural().lexicographical().nullsFirst())
        .compare(outputs, o.outputs, 
            Ordering.<Output>natural().lexicographical().nullsFirst())
        .result();
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
    
    public Query build() {
      return new Query(this);
    }
  }
  
}
