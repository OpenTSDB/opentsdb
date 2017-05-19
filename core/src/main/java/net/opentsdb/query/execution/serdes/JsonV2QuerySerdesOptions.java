// This file is part of OpenTSDB.
// Copyright (C) 2017 The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Serdes options for the Json version 2 serializer.
 * 
 * @since 3.0
 */
public class JsonV2QuerySerdesOptions implements SerdesOptions {
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
  
  /**
   * Default ctor.
   * @param builder Non-null builder.
   */
  protected JsonV2QuerySerdesOptions(final Builder builder) {
    show_tsuids = builder.showTsuids;
    msResolution = builder.msResolution;
    show_query = builder.showQuery;
    show_stats = builder.showStats;
    show_summary = builder.showSummary;
  }
  
  public boolean showTsuids() {
    return show_tsuids;
  }
  
  public boolean msResolution() {
    return msResolution;
  }

  public boolean showQuery() {
    return show_query;
  }

  public boolean showStats() {
    return show_stats;
  }

  public boolean showSummary() {
    return show_summary;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
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
    
    public JsonV2QuerySerdesOptions build() {
      return new JsonV2QuerySerdesOptions(this);
    }
  }
  
}
