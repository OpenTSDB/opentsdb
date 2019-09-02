// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.utils.Comparators;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = SummarizerConfig.Builder.class)
public class SummarizerConfig extends BaseQueryNodeConfig<SummarizerConfig.Builder, SummarizerConfig> {

  /** The non-null and non-empty list of summaries. */
  protected List<String> summaries;

  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;
  
  /** Whether or not we only summarize or pass through as well. */
  private final boolean pass_through;
  
  protected SummarizerConfig(final Builder builder) {
    super(builder);
    if (builder.summaries == null || builder.summaries.isEmpty()) {
      throw new IllegalArgumentException("Summaries cannot be null or "
          + "empty.");
    }
    summaries = builder.summaries;
    infectious_nan = builder.infectiousNan;
    pass_through = builder.pass_through;
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

  /** @return Whether or not we summarize only or pass through too. */
  public boolean passThrough() {
    return pass_through;
  }
  
  @Override
  public int compareTo(SummarizerConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    // is this necessary?
    if (!super.equals(o)) {
      return false;
    }

    final SummarizerConfig sconfig = (SummarizerConfig) o;
    
    final boolean result = Objects.equal(infectious_nan, sconfig.getInfectiousNan());

    if (!result) {
      return false;
    }

    // comparing summaries
    if (!Comparators.ListComparison.equalLists(summaries, sconfig.getSummaries())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  public HashCode buildHashCode() {
    final Hasher hc = Const.HASH_FUNCTION().newHasher()
            .putBoolean(infectious_nan);

    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());

    if (summaries != null) {
      final List<String> keys = Lists.newArrayList(summaries);
      Collections.sort(keys);
      for (final String key : keys) {
        hc.putString(key, Const.UTF8_CHARSET);
      }
      hashes.add(hc.hash());
    }

    return Hashing.combineOrdered(hashes);
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
  public Builder toBuilder() {
    return newBuilder()
        .setSummaries(Lists.newArrayList(summaries))
        .setInfectiousNan(infectious_nan)
        .setId(id);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, SummarizerConfig> {
    @JsonProperty
    private boolean infectiousNan;
    @JsonProperty
    protected List<String> summaries;
    protected boolean pass_through;
    
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
    
    public Builder setPassThrough(final boolean pass_through) {
      this.pass_through = pass_through;
      return this;
    }
    
    public SummarizerConfig build() {
      return new SummarizerConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }
  
}
