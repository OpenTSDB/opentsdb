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

import java.util.NoSuchElementException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

import net.opentsdb.core.Aggregators;
import net.opentsdb.query.expression.NumericFillPolicy;
import net.opentsdb.utils.DateTime;

/**
 * Pojo builder class used for serdes of a metric component of a query
 * @since 2.3
 */
@JsonDeserialize(builder = Metric.Builder.class)
public class Metric extends Validatable {
  /** The name of the metric */
  private String metric;
  
  /** An ID for the metric */
  private String id;
  
  /** The ID of a filter set */
  private String filter;
  
  /** An optional time offset for time over time expressions */
  private String time_offset;
  
  /** An optional aggregation override for the metric */
  private String aggregator;
  
  /** A fill policy for dealing with missing values in the metric */
  private NumericFillPolicy fill_policy;

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Metric(Builder builder) {
    metric = builder.metric;
    id = builder.id;
    filter = builder.filter;
    time_offset = builder.timeOffset;
    aggregator = builder.aggregator;
    fill_policy = builder.fillPolicy;
  }

  /** @return the name of the metric */
  public String getMetric() {
    return metric;
  }

  /** @return an ID for the metric */
  public String getId() {
    return id;
  }

  /** @return the ID of a filter set */
  public String getFilter() {
    return filter;
  }

  /** @return an optional time offset for time over time expressions */
  public String getTimeOffset() {
    return time_offset;
  }

  /** @return an optional aggregation override for the metric */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return a fill policy for dealing with missing values in the metric */
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }
  
  /** @return A new builder for the metric */
  public static Builder Builder() {
    return new Builder();
  }

  /** Validates the metric
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("missing or empty metric");
    }

    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    Query.validateId(id);

    if (time_offset != null) {
      DateTime.parseDateTimeString(time_offset, null);
    }
    
    if (aggregator != null && !aggregator.isEmpty()) {
      try {
        Aggregators.get(aggregator.toLowerCase());
      } catch (final NoSuchElementException e) {
        throw new IllegalArgumentException("Invalid aggregator");
      }
    }

    if (fill_policy != null) {
      fill_policy.validate();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Metric that = (Metric) o;

    return Objects.equal(that.filter, filter)
        && Objects.equal(that.id, id)
        && Objects.equal(that.metric, metric)
        && Objects.equal(that.time_offset, time_offset)
        && Objects.equal(that.aggregator, aggregator)
        && Objects.equal(that.fill_policy, fill_policy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(metric, id, filter, time_offset, aggregator, 
        fill_policy);
  }

  /**
   * A builder for a metric component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String metric;
    @JsonProperty
    private String id;
    @JsonProperty
    private String filter;
    @JsonProperty
    private String timeOffset;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private NumericFillPolicy fillPolicy;
    
    public Builder setMetric(String metric) {
      this.metric = metric;
      return this;
    }

    public Builder setId(String id) {
      Query.validateId(id);
      this.id = id;
      return this;
    }

    public Builder setFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder setTimeOffset(String time_offset) {
      this.timeOffset = time_offset;
      return this;
    }

    public Builder setAggregator(String aggregator) {
      this.aggregator = aggregator;
      return this;
    }

    public Builder setFillPolicy(NumericFillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    public Metric build() {
      return new Metric(this);
    }
  }
}
