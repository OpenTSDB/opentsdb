// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;

import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.utils.JSON;

/**
 * The configuration class for a {@link GroupBy} query node.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = GroupByConfig.Builder.class)
public class GroupByConfig extends BaseQueryNodeConfigWithInterpolators {
  private final Set<String> tag_keys;
  private List<byte[]> encoded_tag_keys;
  private final String aggregator;
  private final boolean infectious_nan;
  private final boolean group_all;
  
  private GroupByConfig(final Builder builder) {
    super(builder);
    if (!builder.group_all && builder.tagKeys == null) {
      throw new IllegalArgumentException("Tag keys cannot be null.");
    }
    if (!builder.group_all && builder.tagKeys.isEmpty()) {
      throw new IllegalArgumentException("Tag keys cannot be empty.");
    }
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (interpolator_configs == null || interpolator_configs.isEmpty()) {
      throw new IllegalArgumentException("Must include at least one"
          + " interpolator config.");
    }
    tag_keys = builder.tagKeys;
    encoded_tag_keys = builder.encoded_tag_keys;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectiousNan;
    group_all = builder.group_all;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The non-empty list of tag keys to group on. */
  public Set<String> getTagKeys() {
    return tag_keys;
  }
  
  /** @return An optional encoded tag key list. May be null if encoding
   * is not used in the pipeline. */
  public List<byte[]> getEncodedTagKeys() {
    return encoded_tag_keys;
  }
  
  public void setEncodedTagKeys(final List<byte[]> encoded_tag_keys) {
    this.encoded_tag_keys = encoded_tag_keys;
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
  
  /** @return Whether or not to group by just the metric or the given tags. */
  public boolean groupAll() {
    return group_all;
  }
  
  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {
    @JsonProperty
    private Set<String> tagKeys;
    private List<byte[]> encoded_tag_keys;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectiousNan;
    @JsonProperty
    private boolean group_all;
    
    /**
     * @param tag_keys A non-null and non-empty set of tag keys to replace any
     * existing tags to group on.
     * @return The builder.
     */
    public Builder setTagKeys(final Set<String> tag_keys) {
      this.tagKeys = tag_keys;
      return this;
    }
    
    /**
     * @param encoded_tag_keys A non-null and non-empty set of encoded 
     * tag keys based on the schema this pipeline will work on. If no
     * schema in use then use {@link #setTagKeys(Set)}.
     * @return The builder.
     */
    public Builder setTagKeys(final List<byte[]> encoded_tag_keys) {
      this.encoded_tag_keys = encoded_tag_keys;
      return this;
    }
    
    /**
     * @param tag_key A non-null and non-empty tag key to group on.
     * @return The builder.
     */
    public Builder addTagKey(final String tag_key) {
      if (tagKeys == null) {
        tagKeys = Sets.newHashSet();
      }
      tagKeys.add(tag_key);
      return this;
    }
    
    /**
     * @param encoded_tag_key A non-null and non-empty encoded tag key 
     * to group on.
     * @return The builder.
     */
    public Builder addTagKey(final byte[] encoded_tag_key) {
      if (encoded_tag_keys == null) {
        encoded_tag_keys = Lists.newArrayList();
      }
      encoded_tag_keys.add(encoded_tag_key);
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
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    /**
     * @param group_all Whether or not to group by all tags (just on metrics)
     * @return The builder.
     */
    public Builder setGroupAll(final boolean group_all) {
      this.group_all = group_all;
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
