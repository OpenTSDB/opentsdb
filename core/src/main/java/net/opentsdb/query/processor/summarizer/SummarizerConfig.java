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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = SummarizerConfig.Builder.class)
public class SummarizerConfig extends BaseQueryNodeConfig {

  /** The non-null and non-empty list of summaries. */
  protected List<String> summaries;

  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;
  
  protected SummarizerConfig(final Builder builder) {
    super(builder);
    if (builder.summaries == null || builder.summaries.isEmpty()) {
      throw new IllegalArgumentException("Summaries cannot be null or "
          + "empty.");
    }
    summaries = builder.summaries;
    infectious_nan = builder.infectiousNan;
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
  public boolean equals(final Object o) {
    // TODO Auto-generated method stub
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof SummarizerConfig)) {
      return false;
    }
    
    return id.equals(((SummarizerConfig) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    private boolean infectiousNan;
    @JsonProperty
    protected List<String> summaries;
    
    Builder() {
      setType(SummarizerFactory.TYPE);
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
