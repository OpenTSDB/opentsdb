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
package net.opentsdb.query;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Objects;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
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
public abstract class BaseQueryNodeConfig implements QueryNodeConfig {

  /** A unique name for this config. */
  protected final String id;
  
  /** The class of an {@link QueryNode} implementation. */
  protected final String type;
  
  /** An optional list of downstream sources. */
  protected List<String> sources;
  
  /** The optional map of overrides. */
  protected final Map<String, String> overrides;
  
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


  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(type), Const.UTF8_CHARSET)
            .hash();
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(hc);

    if (sources != null) {
      final List<String> keys = Lists.newArrayList(sources);
      Collections.sort(keys);
      final Hasher hasher = Const.HASH_FUNCTION().newHasher();
      for (final String key : keys) {
        hasher.putString(key, Const.UTF8_CHARSET);
      }
      hashes.add(hasher.hash());
    }

    return Hashing.combineOrdered(hashes);
  }
  
  /** Base builder for QueryNodeConfig. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static abstract class Builder implements QueryNodeConfig.Builder {

    @JsonProperty
    protected String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private List<String> sources;
    @JsonProperty
    protected Map<String, String> overrides;
    
    /**
     * @param id An ID for this builder.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param type The class of the implementation.
     * @return The builder.
     */
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    /**
     * @param sources An optional list of sources consisting of the IDs 
     * of a nodes in the graph.
     * @return The builder.
     */
    public Builder setSources(final List<String> sources) {
      this.sources = sources;
      return this;
    }
    
    /**
     * @param source A source to pull from for this node.
     * @return The builder.
     */
    public Builder addSource(final String source) {
      if (sources == null) {
        sources = Lists.newArrayListWithExpectedSize(1);
      }
      sources.add(source);
      return this;
    }
    
    /**
     * @param overrides An override map to replace the existing map.
     * @return The builder.
     */
    public Builder setOverrides(final Map<String, String> overrides) {
      this.overrides = overrides;
      return this;
    }
    
    /**
     * @param key An override key to store in the override map.
     * @param value A value to store, overwriting existing values.
     * @return The builder.
     */
    public Builder addOverride(final String key, final String value) {
      if (overrides == null) {
        overrides = Maps.newHashMap();
      }
      overrides.put(key, value);
      return this;
    }
    
    /** @return A config object or an exception if the config failed. */
    public abstract QueryNodeConfig build();
  }
}
