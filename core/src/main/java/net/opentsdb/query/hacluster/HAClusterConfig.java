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
package net.opentsdb.query.hacluster;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.utils.DateTime;

/**
 * The config for a high-availability cluster query wherein the same data
 * is written to multiple locations and the query is executed against all
 * of them, merging the results.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = HAClusterConfig.Builder.class)
public class HAClusterConfig extends BaseQueryNodeConfigWithInterpolators {
  
  /** The non-null and non-empty list of sources to query. */
  private final List<String> sources;
  
  /** The non-null and non-empty aggregator to use to merge results. */
  private final String merge_aggregator;
  
  /** An optional timeout for the secondary (etc) sources. */
  private final String secondary_timeout;
  
  /**
   * Default ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID was null or empty, if the
   * cluster config was null or the execution graph config was null.
   */
  protected HAClusterConfig(final Builder builder) {
    super(builder);
    if (builder.sources == null || builder.sources.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one source.");
    }
    if (Strings.isNullOrEmpty(builder.mergeAggregator)) {
      throw new IllegalArgumentException("Merge aggregator cannot be null.");
    }
    sources = builder.sources;
    merge_aggregator = builder.mergeAggregator;
    secondary_timeout = builder.secondaryTimeout;
    
    // validate the timeout
    if (!Strings.isNullOrEmpty(secondary_timeout)) {
      DateTime.parseDuration(secondary_timeout);
    }
  }

  /** @return The non-null and non-empty list of sources to query. The
   * first entry is primary. */
  public List<String> getSources() {
    return sources;
  }
  
  /** @return The non-null and non-empty aggregator to use to merge results. */
  public String getMergeAggregator() {
    return merge_aggregator;
  }
  
  /** @return An optional timeout for the secondary (etc) sources. */
  public String getSecondaryTimeout() {
    return secondary_timeout;
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
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return System.identityHashCode(this);
  }
  
  /** @return A new builder to construct a Cluster Config */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /** The builder class for cluster configs. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {
    @JsonProperty
    private List<String> sources;
    @JsonProperty
    private String mergeAggregator;
    @JsonProperty
    private String secondaryTimeout;
    
    /**
     * @param sources The list of sources to query from.
     * @return The builder.
     */
    public Builder setSources(final List<String> sources) {
      this.sources = sources;
      return this;
    }
    
    /**
     * @param merge_aggregator The aggregation function to use.
     * @return The builder.
     */
    @JsonIgnore
    public Builder setMergeAggregator(final String merge_aggregator) {
      mergeAggregator = merge_aggregator;
      return this;
    }
    
    /**
     * @param secondary_timeout The amount of time to wait after the
     * primary call before returning data. E.g. "5s".
     * @return The builder.
     */
    public Builder setSecondaryTimeout(final String secondary_timeout) {
      secondaryTimeout = secondary_timeout;
      return this;
    }
    
    /** @return The instantiated ClusterConfig on success or exceptions on 
     * failure. */
    public HAClusterConfig build() {
      return new HAClusterConfig(this);
    }
  }
  
  public static HAClusterConfig parse(final ObjectMapper mapper,
                                      final TSDB tsdb, 
                                      final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("sources");
    if (n != null) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    
    n = node.get("mergeAggregator");
    if (n != null) {
      builder.setMergeAggregator(n.asText());
    }
    
    n = node.get("secondaryTimeout");
    if (n != null) {
      builder.setSecondaryTimeout(n.asText());
    }
    
    n = node.get("interpolatorConfigs");
    for (final JsonNode config : n) {
      JsonNode type_json = config.get("type");
      final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
          QueryInterpolatorFactory.class, 
          type_json == null ? "Default" : type_json.asText());
      if (factory == null) {
        throw new IllegalArgumentException("Unable to find an "
            + "interpolator factory for: " + 
            (type_json == null ? "default" :
             type_json.asText()));
      }
      
      final QueryInterpolatorConfig interpolator_config = 
          factory.parseConfig(mapper, tsdb, config);
      builder.addInterpolatorConfig(interpolator_config);
    }
    
    return (HAClusterConfig) builder.build();
  }
}
