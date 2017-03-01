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
 * Pojo builder class used for serdes of the downsampler component of a query
 * @since 2.3
 */
@JsonDeserialize(builder = Downsampler.Builder.class)
public class Downsampler extends Validatable {
  /** The relative interval with value and unit, e.g. 60s */
  private String interval;
  
  /** The aggregator to use for downsampling */
  private String aggregator;
  
  /** A fill policy for downsampling and working with missing values */
  private NumericFillPolicy fill_policy;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Downsampler(Builder builder) {
    interval = builder.interval;
    aggregator = builder.aggregator;
    fill_policy = builder.fillPolicy;
  }
  
  /** @return A new builder for the downsampler */
  public static Builder Builder() {
    return new Builder();
  }
  
  /** Validates the downsampler
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (interval == null || interval.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty interval");
    }
    DateTime.parseDuration(interval);
    
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty aggregator");
    }
    try {
      Aggregators.get(aggregator.toLowerCase());
    } catch (final NoSuchElementException e) {
      throw new IllegalArgumentException("Invalid aggregator");
    }
    
    if (fill_policy != null) {
      fill_policy.validate();
    }
  }
  
  /** @return the interval for the downsampler */
  public String getInterval() {
    return interval;
  }
  
  /** @return the name of the aggregator to use */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return the fill policy to use */
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Downsampler downsampler = (Downsampler) o;

    return Objects.equal(interval, downsampler.interval)
        && Objects.equal(aggregator, downsampler.aggregator)
        && Objects.equal(fill_policy, downsampler.fill_policy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(interval, aggregator, fill_policy);
  }

  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String interval;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private NumericFillPolicy fillPolicy;
    
    public Builder setInterval(String interval) {
      this.interval = interval;
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
    
    public Downsampler build() {
      return new Downsampler(this);
    }
  }
}
