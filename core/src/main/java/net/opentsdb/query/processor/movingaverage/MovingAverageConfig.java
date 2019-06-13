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
package net.opentsdb.query.processor.movingaverage;

import java.time.temporal.TemporalAmount;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.utils.DateTime;

/**
 * The configuration class for a moving window node.
 * <p>
 * TODO - calendaring?
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = MovingAverageConfig.Builder.class)
public class MovingAverageConfig extends BaseQueryNodeConfig {
  private final int samples;
  private final String interval;
  private final double alpha;
  private final boolean avg_initial;
  private final boolean median;
  private final boolean weighted;
  private final boolean exponential;
  private final boolean infectious_nan;
  private final TemporalAmount duration;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected MovingAverageConfig(final Builder builder) {
    super(builder);
    if (!Strings.isNullOrEmpty(builder.interval) && builder.samples > 0) {
      throw new IllegalArgumentException("Either the interval or number of "
          + "samples must be set but not both.");
    }
    if (Strings.isNullOrEmpty(builder.interval) && builder.samples <= 0) {
      throw new IllegalArgumentException("Either the interval or number of "
          + "samples must be set.");
    }
    if (builder.alpha < 0 || builder.alpha >= 1) {
      throw new IllegalArgumentException("Alpha must be 0 (computed) or "
          + "less than 1.");
    }
    if (builder.median && builder.weighted && builder.exponential) {
      throw new IllegalArgumentException("Please pick one, median, weighted or "
          + "exponential.");
    }
    samples = builder.samples;
    interval = builder.interval;
    alpha = builder.alpha;
    avg_initial = builder.averageInitial;
    median = builder.median;
    weighted = builder.weighted;
    exponential = builder.exponential;
    infectious_nan = builder.infectiousNan;
    if (!Strings.isNullOrEmpty(interval)) {
      duration = DateTime.parseDuration2(interval);
    } else {
      duration = null;
    }
  }
  
  /** @return The number of samples to average. */
  public int getSamples() {
    return samples;
  }
  
  /** @return The raw window interval for averaging. */
  public String getInterval() {
    return interval;
  }
  
  /** @return The parsed interval if the interval was set, null otherwise. */
  public TemporalAmount interval() {
    return duration;
  }
  
  /** @return Whether or not we want the exponential average. */
  public boolean getExponential() {
    return exponential;
  }
  
  /** @return Whether or not we want the median. */
  public boolean getMedian() {
    return median;
  }
  
  /** @return Whether or not we return the WMA. */
  public boolean getWeighted() {
    return weighted;
  }
  
  /** @return An explicit alpha for the EWMA calculation. If 0, the default is 
   * used. */
  public double getAlpha() {
    return alpha;
  }
  
  /** @return Whether or not to average the values and use that as the initial
   * value in EWMA averaging. */
  public boolean getAverageInitial() {
    return avg_initial;
  }

  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  @Override
  public Builder toBuilder() {
    return (Builder) newBuilder()
        .setInterval(interval)
        .setAlpha(alpha)
        .setAverageInitial(avg_initial)
        .setWeighted(weighted)
        .setExponential(exponential)
        .setSamples(samples)
        .setSources(sources)
        .setId(id);
  }
  
  @Override
  public boolean pushDown() {
    return true;
  }
  
  @Override
  public boolean joins() {
    return false;
  }
  
  @Override
  public HashCode buildHashCode() {
 // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
  }

  @Override
  public int compareTo(final QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
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
    if (!(o instanceof MovingAverageConfig)) {
      return false;
    }
    
    return id.equals(((MovingAverageConfig) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    private int samples;
    @JsonProperty
    private String interval;
    @JsonProperty
    private double alpha;
    @JsonProperty
    private boolean averageInitial = true;
    @JsonProperty
    private boolean median;
    @JsonProperty
    private boolean weighted;
    @JsonProperty
    private boolean exponential;
    @JsonProperty
    private boolean infectiousNan;
    
    Builder() {
      setType(MovingAverageFactory.TYPE);
    }
    
    public Builder setSamples(final int samples) {
      this.samples = samples;
      return this;
    }
    
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }
    
    public Builder setAlpha(final double alpha) {
      this.alpha = alpha;
      return this;
    }
    
    public Builder setAverageInitial(final boolean average_initial) {
      this.averageInitial = average_initial;
      return this;
    }
    
    public Builder setMedian(final boolean median) {
      this.median = median;
      return this;
    }
    
    public Builder setWeighted(final boolean weighted) {
      this.weighted = weighted;
      return this;
    }
    
    public Builder setExponential(final boolean exponential) {
      this.exponential = exponential;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    @Override
    public QueryNodeConfig build() {
      return new MovingAverageConfig(this);
    }
    
  }

}
