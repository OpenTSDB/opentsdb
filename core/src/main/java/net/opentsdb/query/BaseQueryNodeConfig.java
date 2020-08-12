// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Objects;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.utils.Comparators;

/**
 * A basic configuration implementation handling the ID and overrides
 * along with a base builder.
 * 
 * @since 3.0
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class BaseQueryNodeConfig<B extends BaseQueryNodeConfig.Builder<B, C>, 
    C extends BaseQueryNodeConfig> implements QueryNodeConfig<B, C> {

  /** A unique name for this config. */
  protected final String id;
  
  /** The class of an {@link QueryNode} implementation. */
  protected final String type;
  
  /** An optional list of downstream sources. */
  protected List<String> sources;
  
  /** The optional map of overrides. */
  protected final Map<String, String> overrides;
  
  /** The cached hash code. */
  protected volatile HashCode cached_hash;
  
  /** Whether or not the node is cacheable in the config graph. */
  protected boolean cacheable;
  
  /** The data sources from this node. */
  protected List<QueryResultId> result_ids;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID was null or empty.
   */
  protected BaseQueryNodeConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    id = builder.id;
    type = builder.type;
    sources = builder.sources == null ? Collections.emptyList() : 
      builder.sources;
    overrides = builder.overrides;
    result_ids = builder.result_ids;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The class of the implementation. */
  public String getType() {
    return type;
  }
    
  /** @return An optional lost of sources mapping to node IDs. */
  public List<String> getSources() {
    return sources;
  }
  
  @Override
  public Map<String, String> getOverrides() {
    return overrides;
  }
  
  @Override
  public boolean readCacheable() {
    // Most nodes can be cached.
    return true;
  }
  
  @Override
  public String getString(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getString(key);
      }
    }
    return value;
  }
  
  @Override
  public int getInt(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Integer.parseInt(value);
  }
  
  @Override
  public long getLong(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Long.parseLong(value);
  }
  
  @Override
  public boolean getBoolean(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getBoolean(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    value = value.trim().toLowerCase();
    return value.equals("true") || value.equals("1") || value.equals("yes");
  }
  
  @Override
  public double getDouble(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Double.parseDouble(value);
  }
  
  @Override
  public boolean hasKey(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return overrides == null ? false : overrides.containsKey(key);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final BaseQueryNodeConfig queryConfig = (BaseQueryNodeConfig) o;

    final boolean result = Objects.equal(getType(), queryConfig.getType())
            && Objects.equal(id, queryConfig.getId());
    if (!result) {
      return false;
    }

    if (!Comparators.ListComparison.equalLists(sources, queryConfig.getSources())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /**
   * <b>WARNING:</b> This method won't set the cached hash.
   * @return A HashCode object for deterministic, non-secure hashing 
   */
  public HashCode buildHashCode() {
    final Hasher hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(type), Const.UTF8_CHARSET);

    if (sources != null) {
      final List<String> keys = Lists.newArrayList(sources);
      Collections.sort(keys);
      for (final String key : keys) {
        hc.putString(key, Const.UTF8_CHARSET);
      }
    }

    return hc.hash();
  }
  
  @Override
  public List<QueryResultId> resultIds() {
    return result_ids == null ? Collections.emptyList() : result_ids;
  }
  
  @Override
  public boolean markedCacheable() {
    return cacheable;
  }
  
  @Override
  public void markCacheable(final boolean cacheable) {
    this.cacheable = cacheable;
  }
  
  /**
   * A method to set the common fields to the given builder so we avoid a little
   * boilerplate.
   * @param builder A non-null builder to set the fieldso n.
   */
  protected void toBuilder(final Builder builder) {
    builder
      .setSources(sources != null ? Lists.newArrayList(sources) : null)
      .setResultIds(result_ids != null ? Lists.newArrayList(result_ids) : null)
      .setOverrides(overrides != null ? Maps.newHashMap(overrides) : null)
      .setType(type)
      .setId(id);
  }
  
  /** Base builder for QueryNodeConfig. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public abstract static class Builder<B extends Builder<B, C>, C extends BaseQueryNodeConfig>
      implements QueryNodeConfig.Builder<B, C> {

    @JsonProperty
    protected String id;
    @JsonProperty
    protected String type;
    @JsonProperty
    protected List<String> sources;
    @JsonProperty
    protected Map<String, String> overrides;
    protected List<QueryResultId> result_ids;

    /**
     * @param id An ID for this builder.
     * @return The builder.
     */
    public B setId(final String id) {
      this.id = id;
      return self();
    }

    /**
     * @param type The class of the implementation.
     * @return The builder.
     */
    public B setType(final String type) {
      this.type = type;
      return self();
    }
    
    /**
     * @param sources An optional list of sources consisting of the IDs 
     * of a nodes in the graph.
     * @return The builder.
     */
    public B setSources(final List<String> sources) {
      this.sources = sources;
      return self();
    }
    
    /**
     * @param source A source to pull from for this node.
     * @return The builder.
     */
    public B addSource(final String source) {
      if (sources == null) {
        sources = Lists.newArrayListWithExpectedSize(1);
      }
      sources.add(source);
      return self();
    }
    
    /**
     * @param overrides An override map to replace the existing map.
     * @return The builder.
     */
    public B setOverrides(final Map<String, String> overrides) {
      this.overrides = overrides;
      return self();
    }
    
    /**
     * @param key An override key to store in the override map.
     * @param value A value to store, overwriting existing values.
     * @return The builder.
     */
    public B addOverride(final String key, final String value) {
      if (overrides == null) {
        overrides = Maps.newHashMap();
      }
      overrides.put(key, value);
      return self();
    }

    @Override
    public B setResultIds(final List<QueryResultId> data_sources) {
      this.result_ids = data_sources;
      return self();
    }
    
    @Override
    public B addResultId(final QueryResultId source) {
      if (result_ids == null) {
        result_ids = Lists.newArrayList(source);
      } else {
        result_ids.add(source);
      }
      return self();
    }
    
  }
}
