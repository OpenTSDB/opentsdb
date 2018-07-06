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
package net.opentsdb.query.interpolation.types.numeric;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;

import net.opentsdb.core.Const;
import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.BaseNumericSummaryFillPolicy;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.interpolation.BaseInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * A configuration for interpolating numeric summaries (e.g. rollups and
 * pre-aggregates).
 * 
 * @since 3.0
 */
public class NumericSummaryInterpolatorConfig extends BaseInterpolatorConfig {

  /** The default numeric fill policy. */
  protected final FillPolicy fill_policy;
  
  /** The default real value fill policy. */
  protected final FillWithRealPolicy real_fill;
  
  /** The map of summaries to fill policies for overriding the default. */
  protected final Map<Integer, FillPolicy> summary_fill_policy_overrides;
  
  /** The map of summaries to real fill policies for overriding the default. */
  protected final Map<Integer, FillWithRealPolicy> summary_real_fill_overrides;
  
  /** Whether or not fills should be synced, meaning a real value for every
   * summary must be present or we return with a fill. */
  protected final boolean sync;
  
  /** The list of expected summary IDs. */
  protected final List<Integer> expected_summaries;
  
  /** An alternative aggregator to use when downsampling or grouping
   * specific summaries. Configured at query time. */
  protected final NumericAggregator component_agg;
    
  /**
   * Package private ctor for use by the builder.
   * @param builder The non-null builder to construct from.
   */
  NumericSummaryInterpolatorConfig(final Builder builder) {
    super(builder);
    if (builder.expected_summaries == null || 
        builder.expected_summaries.isEmpty()) {
      throw new IllegalArgumentException("Expected summaries cannot "
          + "be null or empty.");
    }
    if (builder.fill_policy == null) {
      throw new IllegalArgumentException("Default fill policy cannot be null.");
    }
    if (builder.real_fill == null) {
      throw new IllegalArgumentException("Default real fill policy cannot be null.");
    }
    if (!type.equals(NumericSummaryType.TYPE.toString())) {
      throw new IllegalArgumentException("Type must be " + NumericSummaryType.TYPE);
    }
    fill_policy = builder.fill_policy;
    real_fill = builder.real_fill;
    summary_fill_policy_overrides = builder.summary_fill_policy_overrides;
    summary_real_fill_overrides = builder.summary_real_fill_overrides;
    sync = builder.sync;
    expected_summaries = builder.expected_summaries;
    component_agg = builder.component_agg;
  }
  
  /** @return The default numeric fill policy. */
  public FillPolicy defaultFillPolicy() {
    return fill_policy;
  }
  
  /** @return The default real fill policy. */
  public FillWithRealPolicy defaultRealFillPolicy() {
    return real_fill;
  }
  
  /**
   * Returns the fill policy for this summary ID, either the default or
   * an override if present.
   * @param summary The summary ID to return a fill policy for.
   * @return The override or the default.
   */
  public FillPolicy fillPolicy(final int summary) {
    final FillPolicy policy = summary_fill_policy_overrides == null ? 
        null : summary_fill_policy_overrides.get(summary);
    if (policy == null) {
      return fill_policy;
    }
    return policy;
  }
  
  /**
   * Returns the real fill policy for this summary ID, either the 
   * default or an override if present.
   * @param summary The summary ID to return a fill policy for.
   * @return The override or default
   */
  public FillWithRealPolicy realFillPolicy(final int summary) {
    FillWithRealPolicy policy = summary_real_fill_overrides == null ? 
        null : summary_real_fill_overrides.get(summary);
    if (policy == null) {
      return real_fill;
    }
    return policy;
  }
  
  /**
   * Returns an interpolator for use with a specific summary, working 
   * over the {@link NumericType} data points. It calls 
   * {@link #fillPolicy(int)} and {@link #realFillPolicy(int)} to find 
   * the proper fills.
   * @param summary The summary ID to work with.
   * @return
   */
  public QueryFillPolicy<NumericType> queryFill(final int summary) {
    final NumericInterpolatorConfig config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(fillPolicy(summary))
        .setRealFillPolicy(realFillPolicy(summary))
        .setType(NumericType.TYPE.toString())
        .build();
    return new BaseNumericFillPolicy(config);
  }
  
  /** @return Whether or not the fills are iterated in sync. */
  public boolean sync() {
    return sync;
  }
  
  /** @return The list of expected summary IDs. */
  public List<Integer> expectedSummaries() {
    return expected_summaries;
  }
  
  /** @return An optional alternate aggregator for specific summaries. */
  public NumericAggregator componentAggregator() {
    return component_agg;
  }
  
  /** @return The base numeric fill using the {@link #fillPolicy()}. */
  public QueryFillPolicy<NumericSummaryType> queryFill() {
    return new BaseNumericSummaryFillPolicy(this);
  }
  
  @Override
  public HashCode buildHashCode() {
    final Hasher hasher = Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .putString(type, Const.ASCII_CHARSET)
        .putString(config_type, Const.ASCII_CHARSET)
        .putInt(fill_policy.ordinal())
        .putInt(real_fill.ordinal())
        .putBoolean(sync);
    if (summary_fill_policy_overrides != null && 
        !summary_fill_policy_overrides.isEmpty()) {
      final Map<Integer, FillPolicy> sorted_fills = 
          new TreeMap<Integer, FillPolicy>(summary_fill_policy_overrides);
      for (final Entry<Integer, FillPolicy> entry : sorted_fills.entrySet()) {
        hasher.putInt(entry.getKey())
              .putInt(entry.getValue().ordinal());
      }
    }
    if (summary_real_fill_overrides != null && 
        !summary_real_fill_overrides.isEmpty()) {
      final Map<Integer, FillWithRealPolicy> sorted_fills = 
          new TreeMap<Integer, FillWithRealPolicy>(summary_real_fill_overrides);
      for (final Entry<Integer, FillWithRealPolicy> entry : sorted_fills.entrySet()) {
        hasher.putInt(entry.getKey())
              .putInt(entry.getValue().ordinal());
      }
    }
    if (expected_summaries != null && !expected_summaries.isEmpty()) {
      Collections.sort(expected_summaries);
      for (final int expected : expected_summaries) {
        hasher.putInt(expected);
      }
    }
    return hasher.hash();
  }
  
  @Override
  public int compareTo(final QueryInterpolatorConfig o) {
    if (o == null) {
      return 1;
    }
    if (o == this) {
      return 0;
    }
    if (!(o instanceof NumericSummaryInterpolatorConfig)) {
      return 1;
    }
    
    return ComparisonChain.start()
        .compare(id, ((NumericSummaryInterpolatorConfig) o).id)
        .compare(type, ((NumericSummaryInterpolatorConfig) o).type)
        .compare(config_type, ((NumericSummaryInterpolatorConfig) o).config_type)
        .compare(fill_policy, ((NumericSummaryInterpolatorConfig) o).fill_policy)
        .compare(real_fill, ((NumericSummaryInterpolatorConfig) o).real_fill)
        .compare(sync, ((NumericSummaryInterpolatorConfig) o).sync)
        .compare(expected_summaries, ((NumericSummaryInterpolatorConfig) o).expected_summaries, 
            Ordering.<Integer>natural().lexicographical().nullsFirst())
        .compare(summary_fill_policy_overrides, 
            ((NumericSummaryInterpolatorConfig) o).summary_fill_policy_overrides,
            FILL_CMP)
        .compare(summary_real_fill_overrides, 
            ((NumericSummaryInterpolatorConfig) o).summary_real_fill_overrides, 
            REAL_FILL_CMP)
        .result();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof NumericSummaryInterpolatorConfig)) {
      return false;
    }
    
    final NumericSummaryInterpolatorConfig other = (NumericSummaryInterpolatorConfig) o;
    return Objects.equals(id, other.id) &&
           Objects.equals(type, other.type) &&
           Objects.equals(config_type, other.config_type) && 
           Objects.equals(fill_policy, other.fill_policy) &&
           Objects.equals(real_fill, other.real_fill) && 
           Objects.equals(sync, other.sync) && 
           Objects.equals(expected_summaries, other.expected_summaries) &&
           Objects.equals(summary_fill_policy_overrides, other.summary_fill_policy_overrides) &&
           Objects.equals(summary_real_fill_overrides, other.summary_real_fill_overrides);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseInterpolatorConfig.Builder {
    private FillPolicy fill_policy;
    private FillWithRealPolicy real_fill;
    private Map<Integer, FillPolicy> summary_fill_policy_overrides;
    private Map<Integer, FillWithRealPolicy> summary_real_fill_overrides;
    private boolean sync;
    private List<Integer> expected_summaries;
    private NumericAggregator component_agg;
    
    /**
     * @param fill_policy A non-null numeric fill policy.
     * @return The builder.
     */
    public Builder setDefaultFillPolicy(final FillPolicy fill_policy) {
      this.fill_policy = fill_policy;
      return this;
    }
    
    /**
     * @param real_fill A non-null real fill policy.
     * @return The builder.
     */
    public Builder setDefaultRealFillPolicy(final FillWithRealPolicy real_fill) {
      this.real_fill = real_fill;
      return this;
    }
    
    /**
     * @param summary_fill_policy_overrides A map of summary IDs to 
     * fill policy overrides.
     * @return The builder.
     */
    public Builder setFillPolicyOverrides(
        final Map<Integer, FillPolicy> summary_fill_policy_overrides) {
      this.summary_fill_policy_overrides = summary_fill_policy_overrides;
      return this;
    }
    
    /**
     * @param summary_real_fill_overrides A map of summary IDs to real
     * fill policy overrides.
     * @return The builder.
     */
    public Builder setRealFillPolicyOverrides(
        final Map<Integer, FillWithRealPolicy> summary_real_fill_overrides) {
      this.summary_real_fill_overrides = summary_real_fill_overrides;
      return this;
    }
    
    /**
     * Adds a fill policy override to the map for the summary.
     * @param summary A summary ID.
     * @param fill_policy A non-null fill policy.
     * @return The builder.
     * @throws IllegalArgumentException if the policy was null.
     */
    public Builder addFillPolicyOverride(final int summary, 
                                         final FillPolicy fill_policy) {
      if (fill_policy == null) {
        throw new IllegalArgumentException("Policy cannot be null.");
      }
      if (summary_fill_policy_overrides == null) {
        summary_fill_policy_overrides = Maps.newHashMapWithExpectedSize(1);
      }
      summary_fill_policy_overrides.put(summary, fill_policy);
      return this;
    }
    
    /**
     * Adds a real fill policy override to the map for the summary.
     * @param summary A summary ID.
     * @param fill_policy A non-null fill policy.
     * @return The builder.
     * @throws IllegalArgumentException if the policy was null.
     */
    public Builder addRealFillPolicyOverride(final int summary, 
                                             final FillWithRealPolicy fill_policy) {
      if (summary_real_fill_overrides == null) {
        summary_real_fill_overrides = Maps.newHashMapWithExpectedSize(1);
      }
      summary_real_fill_overrides.put(summary, fill_policy);
      return this;
    }
    
    /**
     * @param sync Whether or not the summaries must be filled in sync.
     * @return The builder.
     */
    public Builder setSync(final boolean sync) {
      this.sync = sync;
      return this;
    }
    
    /**
     * @param expected_summaries A list of expected summaries to fill.
     * @return The builder.
     */
    public Builder setExpectedSummaries(
        final List<Integer> expected_summaries) {
      this.expected_summaries = expected_summaries;
      return this;
    }
    
    /**
     * @param summary An expected summary.
     * @return The builder.
     */
    public Builder addExpectedSummary(final int summary) {
      if (expected_summaries == null) {
        expected_summaries = Lists.newArrayListWithExpectedSize(1);
      }
      expected_summaries.add(summary);
      return this;
    }
    
    /**
     * @param component_agg An optional alternate aggregator.
     * @return The builder.
     */
    public Builder setComponentAggregator(
        final NumericAggregator component_agg) {
      this.component_agg = component_agg;
      return this;
    }
    
    /** @return An instantiated interpolator config. */
    public NumericSummaryInterpolatorConfig build() {
      return new NumericSummaryInterpolatorConfig(this);
    }
  }
  
  private static final MapComparator<Integer, FillPolicy> FILL_CMP = 
      new MapComparator<Integer, FillPolicy>();
  private static final MapComparator<Integer, FillWithRealPolicy> REAL_FILL_CMP = 
      new MapComparator<Integer, FillWithRealPolicy>();
}