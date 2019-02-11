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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
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
  private final boolean merge_ids;
  private final boolean full_merge;
  
  private GroupByConfig(final Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (interpolator_configs == null || interpolator_configs.isEmpty()) {
      throw new IllegalArgumentException("Must include at least one"
          + " interpolator config.");
    }
    tag_keys = builder.tagKeys == null ? Collections.emptySet() : builder.tagKeys;
    encoded_tag_keys = builder.encoded_tag_keys;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectiousNan;
    merge_ids = builder.mergeIds;
    full_merge = builder.fullMerge;
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
  
  /** @param encoded_tag_keys The encoded keys to join on. */
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
  
  /** @return Whether or not to merge the IDs or just take the group tags. */
  public boolean getMergeIds() {
    return merge_ids;
  }
  
  /** @return Whether or not to compute disjoint tags. */
  public boolean getFullMerge() {
    return full_merge;
  }
  
  @Override
  public Builder toBuilder() {
    return (Builder) new Builder()
        .setTagKeys(Sets.newHashSet(tag_keys))
        .setAggregator(aggregator)
        .setInfectiousNan(infectious_nan)
        .setMergeIds(merge_ids)
        .setFullMerge(full_merge)
        .setInterpolatorConfigs(Lists.newArrayList(interpolator_configs.values()))
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
    if (!(o instanceof GroupByConfig)) {
      return false;
    }
    
    return id.equals(((GroupByConfig) o).id);
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
    private boolean mergeIds;
    @JsonProperty
    private boolean fullMerge;
    
    Builder() {
      setType(GroupByFactory.TYPE);
    }
    
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
     * @param merge_ids Whether or not to merge IDs or just use the tags
     * identified in the group-by.
     * @return The builder.
     */
    public Builder setMergeIds(final boolean merge_ids) {
      mergeIds = merge_ids;
      return this;
    }
    
    /**
     * @param merge_ids Whether or not to create a full merge with
     * disjoint tags.
     * @return The builder.
     */
    public Builder setFullMerge(final boolean full_merge) {
      fullMerge = full_merge;
      return this;
    }
    
    /** @return The constructed config.
     * @throws IllegalArgumentException if a required parameter is missing or
     * invalid. */
    public GroupByConfig build() {
      return new GroupByConfig(this);
    }
  }
  
  public static GroupByConfig parse(final ObjectMapper mapper,
      final TSDB tsdb, 
      final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("tagKeys");
    if (n != null) {
      for (final JsonNode key : n) {
        builder.addTagKey(key.asText());
      }
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    
    n = node.get("aggregator");
    if (n != null) {
      builder.setAggregator(n.asText());
    }
    
    n = node.get("infectiousNan");
    if (n != null) {
      builder.setInfectiousNan(n.asBoolean());
    }
    
    n = node.get("mergeIds");
    if (n != null) {
      builder.setMergeIds(n.asBoolean());
    }
    
    n = node.get("fullMerge");
    if (n != null) {
      builder.setFullMerge(n.asBoolean());
    }
    
    n = node.get("interpolatorConfigs");
    for (final JsonNode config : n) {
      JsonNode type_json = config.get("type");
      final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
          QueryInterpolatorFactory.class, 
          type_json == null || type_json.isNull() ? null : type_json.asText());
      if (factory == null) {
        throw new IllegalArgumentException("Unable to find an "
            + "interpolator factory for: " + 
            (type_json == null ? null : type_json.asText()));
      }
      
      final QueryInterpolatorConfig interpolator_config = 
          factory.parseConfig(mapper, tsdb, config);
      builder.addInterpolatorConfig(interpolator_config);
    }
    
    n = node.get("sources");
    if (n != null && !n.isNull()) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    
    return (GroupByConfig) builder.build();
  }
}
