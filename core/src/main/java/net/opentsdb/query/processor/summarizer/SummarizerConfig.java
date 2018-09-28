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
package net.opentsdb.query.processor.summarizer;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.query.QueryNodeConfig;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = SummarizerConfig.Builder.class)
public class SummarizerConfig implements QueryNodeConfig {

  /** The ID. */
  protected String id;
  
  /** The non-null and non-empty list of summaries. */
  protected List<String> summaries;

  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;
  
  protected SummarizerConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("The ID cannot be null.");
    }
    if (builder.summaries == null || builder.summaries.isEmpty()) {
      throw new IllegalArgumentException("Summaries cannot be null or "
          + "empty.");
    }
    id = builder.id;
    summaries = builder.summaries;
    infectious_nan = builder.infectiousNan;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The non-null and non-empty list of summaries to record. */
  public List<String> getSummaries() {
    return summaries;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().hashInt(System.identityHashCode(this));
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Map<String, String> getOverrides() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getString(Configuration config, String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getInt(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getBoolean(Configuration config, String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public double getDouble(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean hasKey(String key) {
    // TODO Auto-generated method stub
    return false;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    protected String id;
    @JsonProperty
    private boolean infectiousNan;
    @JsonProperty
    protected List<String> summaries;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setSummaries(final List<String> summaries) {
      this.summaries = summaries;
      return this;
    }
    
    public Builder addSummary(final String summary) {
      if (summaries == null) {
        summaries = Lists.newArrayList();
      }
      summaries.add(summary);
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      infectiousNan = infectious_nan;
      return this;
    }
    
    public QueryNodeConfig build() {
      return new SummarizerConfig(this);
    }
  }
  
}
