// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.topn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.HashCode;

import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

/**
 * A config for TopN processor nodes.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TopNConfig.Builder.class)
public class TopNConfig extends BaseQueryNodeConfig {

  /** The max number of time series to return. */
  private int count;
  
  /** Whether or not to return the top (highest values) or bottom (lowest
   * values) time series post aggregation. */
  private boolean top;
  
  /** The aggregator to use to sort the series. */
  private String aggregator;
  
  /** Whether or not NaNs are infectious. */
  private boolean infectious_nan;
  
  /**
   * Protected ctor.
   * @param builder Non-null builder.
   */
  protected TopNConfig(final Builder builder) {
    super(builder);
    count = builder.count;
    top = builder.top;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectiousNan;
  }
  
  /** @return The maximum number of time series to return. */
  public int getCount() {
    return count;
  }
  
  /** @return Whether or not to return the top or bottom series. */
  public boolean getTop() {
    return top;
  }
  
  /** @return The aggregation function to use. */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    private int count;
    @JsonProperty
    private boolean top;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectiousNan;
    
    public Builder setCount(final int count) {
      this.count = count;
      return this;
    }
    
    public Builder setTop(final boolean top) {
      this.top = top;
      return this;
    }
    
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    @Override
    public QueryNodeConfig build() {
      return new TopNConfig(this);
    }
    
  }

}
