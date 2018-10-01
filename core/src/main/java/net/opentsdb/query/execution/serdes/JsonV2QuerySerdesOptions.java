// This file is part of OpenTSDB.
// Copyright (C) 2017-2018 The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import net.opentsdb.query.serdes.SerdesOptions;

/**
 * Serdes options for the Json version 2 serializer.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = JsonV2QuerySerdesOptions.Builder.class)
public class JsonV2QuerySerdesOptions extends BaseSerdesOptions {
  /** Whether or not to show the TSUIDs. */
  private boolean show_tsuids;
  
  /** Whether or not to display the results in milliseconds. */
  private boolean msResolution;
  
  /** Whether or not to show the TS Query with the responses. */
  private boolean show_query;
  
  /** Whether or not to show the stats. */
  private boolean show_stats;
  
  /** Whether or not to show the summary. */
  private boolean show_summary;
  
  /** The number of time series necessary to switch to parallel mode. */
  private int parallel_threshold;
  
  /**
   * Default ctor.
   * @param builder Non-null builder.
   */
  protected JsonV2QuerySerdesOptions(final Builder builder) {
    super(builder);
    show_tsuids = builder.showTsuids;
    msResolution = builder.msResolution;
    show_query = builder.showQuery;
    show_stats = builder.showStats;
    show_summary = builder.showSummary;
    parallel_threshold = builder.parallelThreshold;
  }
  
  public boolean getShowTsuids() {
    return show_tsuids;
  }
  
  public boolean getMsResolution() {
    return msResolution;
  }

  public boolean getShowQuery() {
    return show_query;
  }

  public boolean getShowStats() {
    return show_stats;
  }

  public boolean getShowSummary() {
    return show_summary;
  }
  
  public int getParallelThreshold() {
    return parallel_threshold;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseSerdesOptions.Builder {
    @JsonProperty
    private boolean showTsuids;
    @JsonProperty
    private boolean msResolution;
    @JsonProperty
    private boolean showQuery;
    @JsonProperty
    private boolean showStats;
    @JsonProperty
    private boolean showSummary;
    @JsonProperty
    private int parallelThreshold;
    
    public Builder setShowTsuids(final boolean showTsuids) {
      this.showTsuids = showTsuids;
      return this;
    }
    
    public Builder setMsResolution(final boolean msResolution) {
      this.msResolution = msResolution;
      return this;
    }
    
    public Builder setShowQuery(final boolean showQuery) {
      this.showQuery = showQuery;
      return this;
    }
    
    public Builder setShowStats(final boolean showStats) {
      this.showStats = showStats;
      return this;
    }
    
    public Builder setShowSummary(final boolean showSummary) {
      this.showSummary = showSummary;
      return this;
    }
    
    public Builder setParallelThreshold(final int parallelThreshold) {
      this.parallelThreshold = parallelThreshold;
      return this;
    }
    
    public SerdesOptions build() {
      return new JsonV2QuerySerdesOptions(this);
    }
  }
  
}
