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
import net.opentsdb.utils.DateTime;

/**
 * Pojo builder class used for serdes of the timespan component of a query
 * @since 2.3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Timespan.Builder.class)
public class Timespan extends Validatable {
  /** User given start date/time, could be relative or absolute */
  private String start;
  
  /** User given end date/time, could be relative, absolute or empty */
  private String end;
  
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;

  /** An optional downsampler for all queries */
  private Downsampler downsampler;
  
  /** The global aggregator to use */
  private String aggregator; 
  
  /** Whether or not to compute a rate */
  private boolean rate;

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Timespan(Builder builder) {
    start = builder.start;
    end = builder.end;
    timezone = builder.timezone;
    downsampler = builder.downsampler;
    aggregator = builder.aggregator;
    rate = builder.rate;
  }
  
  /** @return user given start date/time, could be relative or absolute */
  public String getStart() {
    return start;
  }

  /** @return user given end date/time, could be relative, absolute or empty */
  public String getEnd() {
    return end;
  }

  /** @return user's timezone used for converting absolute human readable dates */
  public String getTimezone() {
    return timezone;
  }

  /** @return an optional downsampler for all queries */
  public Downsampler getDownsampler() {
    return downsampler;
  }
  
  /** @return the global aggregator to use */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return whether or not to compute a rate */
  public boolean isRate() {
    return rate;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Timespan timespan = (Timespan) o;

    return Objects.equal(timespan.downsampler, downsampler)
        && Objects.equal(timespan.end, end)
        && Objects.equal(timespan.start, start)
        && Objects.equal(timespan.timezone, timezone)
        && Objects.equal(timespan.aggregator, aggregator)
        && Objects.equal(timespan.rate, rate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end, timezone, downsampler, aggregator, rate);
  }

  /** @return A new builder for the downsampler */
  public static Builder Builder() {
    return new Builder();
  }

  /** Validates the timespan
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (start == null || start.isEmpty()) {
      throw new IllegalArgumentException("missing or empty start");
    }
    DateTime.parseDateTimeString(start, timezone);
    
    if (end != null && !end.isEmpty()) {
      DateTime.parseDateTimeString(end, timezone);
    }
    
    if (downsampler != null) {
      downsampler.validate();
    }
    
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty aggregator");
    }
    
    try {
      Aggregators.get(aggregator.toLowerCase());
    } catch (final NoSuchElementException e) {
      throw new IllegalArgumentException("Invalid aggregator");
    }
  }
  
  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String start;

    @JsonProperty
    private String end;

    @JsonProperty
    private String timezone;

    @JsonProperty
    private Downsampler downsampler;
    
    @JsonProperty
    private String aggregator;
    
    @JsonProperty
    private boolean rate;
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }

    public Builder setTimezone(final String timezone) {
      this.timezone = timezone;
      return this;
    }

    public Builder setDownsampler(final Downsampler downsample) {
      this.downsampler = downsample;
      return this;
    }

    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    public Builder setRate(final boolean rate) {
      this.rate = rate;
      return this;
    }
    
    public Timespan build() {
      return new Timespan(this);
    }
  }
  
}
