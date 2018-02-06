// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import net.opentsdb.query.QueryIteratorInterpolatorConfig;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryNodeConfig;

/**
 * The configuration class for a {@link GroupBy} query node.
 * 
 * @since 3.0
 */
public class GroupByConfig implements QueryNodeConfig {
  private final String id;
  private final Set<String> tag_keys;
  private final String aggregator;
  private final boolean infectious_nan;
  private final QueryIteratorInterpolatorFactory interpolator;
  private final QueryIteratorInterpolatorConfig interpolator_config;
  
  private GroupByConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    if (builder.tag_keys == null) {
      throw new IllegalArgumentException("Tag keys cannot be null.");
    }
    if (builder.tag_keys.isEmpty()) {
      throw new IllegalArgumentException("Tag keys cannot be empty.");
    }
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (builder.interpolator == null) {
      throw new IllegalArgumentException("Interpolator factory cannot be null.");
    }
    id = builder.id;
    tag_keys = builder.tag_keys;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectious_nan;
    interpolator = builder.interpolator;
    interpolator_config = builder.interpolator_config;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The non-empty list of tag keys to group on. */
  public Set<String> getTagKeys() {
    return tag_keys;
  }
  
  /** @return The non-null and non-empty aggregation function name. */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  /** @return The non-null interpolator factory. */
  public QueryIteratorInterpolatorFactory getInterpolator() {
    return interpolator;
  }
  
  /** @return The optional interpolator config. May be null. */
  public QueryIteratorInterpolatorConfig getInterpolatorConfig() {
    return interpolator_config;
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private String id;
    private Set<String> tag_keys;
    private String aggregator;
    private boolean infectious_nan;
    private QueryIteratorInterpolatorFactory interpolator;
    private QueryIteratorInterpolatorConfig interpolator_config;
    
    /**
     * @param id A non-null and on-empty Id for the group by function.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param tag_keys A non-null and non-empty set of tag keys to replace any
     * existing tags to group on.
     * @return The builder.
     */
    public Builder setTagKeys(final Set<String> tag_keys) {
      this.tag_keys = tag_keys;
      return this;
    }
    
    /**
     * @param tag_key A non-null and non-empty tag key to group on.
     * @return The builder.
     */
    public Builder addTagKey(final String tag_key) {
      if (tag_keys == null) {
        tag_keys = Sets.newHashSet();
      }
      tag_keys.add(tag_key);
      return this;
    }
    
    /**
     * @param aggregator A non-null and non-empty aggregation function.
     * @return The builder.
     */
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectious_nan = infectious_nan;
      return this;
    }
    
    /**
     * @param interpolator The non-null interpolator factory to use.
     * @return The builder.
     */
    public Builder setQueryIteratorInterpolatorFactory(
        final QueryIteratorInterpolatorFactory interpolator) {
      this.interpolator = interpolator;
      return this;
    }
    
    /**
     * @param interpolator_config An optional interpolator config.
     * @return The builder.
     */
    public Builder setQueryIteratorInterpolatorConfig(
        final QueryIteratorInterpolatorConfig interpolator_config) {
      this.interpolator_config = interpolator_config;
      return this;
    }
    
    /** @return The constructed config.
     * @throws IllegalArgumentException if a required parameter is missing or
     * invalid. */
    public GroupByConfig build() {
      return new GroupByConfig(this);
    }
  }
}
