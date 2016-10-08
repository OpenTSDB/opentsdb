// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

import net.opentsdb.utils.JSON;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pojo builder class used for serdes of the expression query
 * @since 2.3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Query.Builder.class)
public class Query extends Validatable {
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

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Query(Builder builder) {
    this.name = builder.name;
    this.time = builder.time;
    this.filters = builder.filters;
    this.metrics = builder.metrics;
    this.expressions = builder.expressions;
    this.outputs = builder.outputs;
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
    return filters;
  }

  /** @return a list of metrics */
  public List<Metric> getMetrics() {
    return metrics;
  }

  /** @return a list of expressions */
  public List<Expression> getExpressions() {
    return expressions;
  }

  /** @return a list of outputs */
  public List<Output> getOutputs() {
    return outputs;
  }

  /** @return A new builder for the query */
  public static Builder Builder() {
    return new Builder();
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

    for (Filter filter : filters) {
      if (filter_ids.contains(filter.getId())) {
        throw new IllegalArgumentException("duplicated filter id: "
            + filter.getId());
      }
      filter_ids.add(filter.getId());
    }
    
    for (Expression expression : expressions) {
      if (variable_ids.contains(expression.getId())) {
        throw new IllegalArgumentException("Duplicated variable or expression id: "
            + expression.getId());
      }
      variable_ids.add(expression.getId());
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
  public boolean equals(Object o) {
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
    return Objects.hashCode(name, time, filters, metrics, expressions, outputs);
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

    public Builder() { }

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setTime(final Timespan time) {
      this.time = time;
      return this;
    }

    public Builder setFilters(final List<Filter> filters) {
      this.filters = filters;
      return this;
    }

    public Builder setMetrics(final List<Metric> metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder setExpressions(final List<Expression> expressions) {
      this.expressions = expressions;
      return this;
    }

    public Builder setOutputs(final List<Output> outputs) {
      this.outputs = outputs;
      return this;
    }

    public Query build() {
      return new Query(this);
    }
  }
  
}
